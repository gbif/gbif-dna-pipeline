# Nucleotide Sequence Processor

Spark UDFs for cleaning/validating DNA/RNA sequences and for looking up GBIF vocabulary terms.

This README was updated to reflect newer UDF registrations and job CLI usage:
- `processSequence` UDF now accepts a Java Map<String,String> for options (second argument)
  and is registered via `ProcessSequenceUdfWithMapOptions.register(spark)`.
- Vocabulary lookup UDFs are provided by `GbifVocabularyLookupUdf` and should be registered
  with a broadcasted small `VocabConfig`. Executors lazily build their own lookup instances.
- Jobs use `picocli` for arguments (see `LookupTargetGeneJob`).

## Usage

### Java Registration (ProcessSequence)

The sequence UDF is now registered from `ProcessSequenceUdfWithMapOptions` and accepts
an options map as the second argument. This is a Java UDF2<String, Map<String,String>, Row>
that returns a Struct (see `ProcessSequenceUdfSchema.RETURN_TYPE` in the code).

```java
import org.apache.spark.sql.SparkSession;
import org.gbif.dna.spark.udf.ProcessSequenceUdfWithMapOptions;

SparkSession spark = SparkSession.builder().appName("example").getOrCreate();

// Register the UDF (name: processSequence)
ProcessSequenceUdfWithMapOptions.register(spark);

// Use in SQL with default options (null = use defaults)
spark.sql("SELECT processSequence(dna_sequence, null) as result FROM sequences");

// Use with custom options map (Java Map<String,String>)
// Example: change N-run capping and disable end trimming
spark.sql("SELECT processSequence(dna_sequence, map('maxNRunsCap','10','trimEnds','false')) as result FROM sequences");
```

The UDF returns a STRUCT with the following fields (same semantics as before):
- seqId, rawSequence, sequence, sequenceLength, nonIupacFraction, nonACGTNFraction,
  nFraction, nNrunsCapped, gcContent, naturalLanguageDetected, endsTrimmed,
  gapsOrWhitespaceRemoved, nucleotideSequenceID, invalid

(Implementation exposes the STRUCT type as `ProcessSequenceUdfSchema.RETURN_TYPE`.)

### SQL Example (processSequence)

```sql
-- Create nucleotide sequence table using the processSequence UDF with default options
CREATE TABLE prod.processed_sequences AS
SELECT
  gbifid,
  datasetkey,
  result.sequence as cleaned_sequence,
  result.sequenceLength,
  result.gcContent,
  result.nucleotideSequenceID,
  result.invalid
FROM (
  SELECT
    gbifid,
    datasetkey,
    processSequence(dna_sequence, null) as result
  FROM prod.occurrence_sequences
  WHERE dna_sequence IS NOT NULL
) t;
```

You can pass options as a SQL map() with string keys and values. The UDF will parse
options like `maxNRunsCap` (int) and `trimEnds` (boolean) from that map.

## Vocabulary Lookup UDF

The `GbifVocabularyLookupUdf` provides vocabulary term lookup using GBIF's vocabulary API.
Important notes:
- Don't broadcast `InMemoryVocabularyLookup` itself: it contains non-serializable cache internals.
  Instead broadcast a small serializable `VocabConfig` (base URL + vocabulary name). Executors
  will lazily build an `InMemoryVocabularyLookup` from that config on first use.
- Two UDFs are provided:
  - `gbifVocabularyLookup(value)` -> returns the matched concept name (STRING) or null
  - `gbifVocabularyLookupLineage(value)` -> returns a STRUCT with fields `{concept: STRING, lineage: ARRAY<STRING>}`

### Java Usage (registering the UDFs)

```java
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.gbif.vocabulary.spark.udf.GbifVocabularyLookupUdf;
import org.gbif.vocabulary.spark.udf.GbifVocabularyLookupUdf.VocabConfig;

SparkSession spark = SparkSession.builder().appName("vocab-lookup").getOrCreate();

// Create and broadcast the small vocabulary config (serializable)
VocabConfig cfg = new VocabConfig("https://api.gbif.org/v1/vocabularies", "LifeStage");
Broadcast<VocabConfig> broadcastConfig = spark.sparkContext().broadcast(
    cfg,
    scala.reflect.ClassTag$.MODULE$.apply(VocabConfig.class)
);

// Register the UDFs
GbifVocabularyLookupUdf.register(spark, "gbifVocabularyLookup", broadcastConfig);
GbifVocabularyLookupUdf.registerWithLineage(spark, "gbifVocabularyLookupLineage", broadcastConfig);

// Use in SQL
spark.sql("SELECT gbifVocabularyLookup(raw_lifestage) AS lifestage FROM occurrences");
// Or get the full struct with concept + lineage
spark.sql("SELECT gbifVocabularyLookupLineage(raw_lifestage) AS v FROM occurrences");

// After job finishes, unpersist broadcast to free memory
broadcastConfig.unpersist(true);
```

### STRUCT returned by `gbifVocabularyLookupLineage`

The lineage UDF returns a Spark StructType equivalent to:

STRUCT<
  concept: STRING,
  lineage: ARRAY<STRING>
>

Usage in SQL to extract fields:
```sql
SELECT
  (gbifVocabularyLookupLineage(raw_target_gene)).concept AS matched_concept,
  (gbifVocabularyLookupLineage(raw_target_gene)).lineage[0] AS top_lineage_element
FROM ...
```

## Jobs and CLI (picocli)

Several jobs ship with the project and use `picocli` for parsing arguments. Example: `LookupTargetGeneJob`.

LookupTargetGeneJob options (important ones):
- `--source-table` / `-s` : Source Iceberg table (required)
- `--target-table` / `-t` : Target Iceberg table (required)
- `--vocab-url` / `-u` : GBIF vocab API base URL (required)
- `--vocab-name` / `-n` : Vocabulary name (required)
- `--source-column` / `-c` : Source column name containing raw value (required)
- `--result-column` / `-r` : (optional) name for the concept name (STRING)
- `--result-concept-column` : (optional) name for the struct with concept+lineage

At least one of `--result-column` or `--result-concept-column` must be provided.

Example spark-submit for `LookupTargetGeneJob` (driver/executor jars must contain compiled classes):

```bash
spark-submit \
  --class org.gbif.dna.spark.jobs.LookupTargetGeneJob \
  --master k8s://https://<k8s-api-server> \
  --deploy-mode client \
  --jars hdfs://<namenode>/user/spark/jars/gbif-dna-spark-udf-1.0-SNAPSHOT.jar,hdfs://<namenode>/user/spark/jars/gbif-dna-core-1.0-SNAPSHOT.jar \
  --conf spark.kubernetes.container.image=your.registry/spark:3.5.1 \
  hdfs://<namenode>/user/spark/apps/gbif-dna-spark-1.0-SNAPSHOT.jar \
  --source-table prod.occurrence_ext_gbif_dnaderiveddata \
  --target-table prod.test_targetgene \
  --vocab-url https://api.gbif.org/v1/vocabularies \
  --vocab-name target_gene \
  --source-column target_gene \
  --result-column target_gene_simple \
  --result-concept-column target_gene_struct
```

Notes:
- You should provide the small `gbif-dna-spark-udf` JAR and `gbif-dna-core` JAR in the classpath/--jars or in the image. Building a "fat"/assembly jar (containing compile-scope deps) is an option but be careful to avoid conflicting Spark/Hadoop dependencies.
- Executors will create their own `InMemoryVocabularyLookup` instances lazily from the broadcasted `VocabConfig`. The lookup code uses a per-executor cache and is not itself serialized.

## Using from code (example processing flow)

Typical flow from a job or interactive session:
1. Create SparkSession with required Iceberg/Spark options
2. Broadcast a `VocabConfig` if using vocabulary lookup
3. Register UDFs (`ProcessSequenceUdfWithMapOptions.register`, `GbifVocabularyLookupUdf.register`, etc.)
4. Run a SQL `SELECT` that calls `processSequence(...)`, `gbifVocabularyLookup(...)` and/or `gbifVocabularyLookupLineage(...)`
5. Write result to Iceberg with `.saveAsTable(...)`
6. Unpersist broadcast variable(s) when done

## Building and running on Kubernetes / spark-submit

### Build JAR

```bash
mvn clean package -DskipTests
```

### Make JAR available to pods
Either include the JAR in your custom image or upload it to HDFS/S3/GCS and reference it with `--jars`.

### Troubleshooting
- ClassNotFoundException: Ensure the JARs with your code and any necessary non-Spark third-party libs are available to both driver and executors
- NotSerializableException when broadcasting: broadcast only small serializable configs (e.g., `VocabConfig`), not caches or non-serializable caches (cache2k internals).
- If running on Java 17+, some older Hadoop/Hive libraries may attempt reflection into JDK internals; ensure compatible versions or add `--add-opens` if necessary for local testing only.

## Running Unit Tests

```bash
mvn test
```

To run a specific test class, for example:

```bash
mvn test -Dtest=ProcessSequenceUdfTest
```


## Contributing / Notes
- The code now uses a per-executor lazy-initialized `InMemoryVocabularyLookup` built from a broadcasted `VocabConfig` to avoid serialization problems with cache internals.
- `ProcessSequenceUdfWithMapOptions` uses a transient ThreadLocal/processor instance per-executor to avoid reflection and Kryo/Java serialization issues.
- If you need a convenience helper that converts `LookupConcept` to a Spark struct, use `GbifVocabularyLookupUdf.fromLookupConcept(...)` which returns STRUCT<concept:STRING,lineage:ARRAY<STRING>> for easy SQL use.

If you'd like, I can also update the top-level `README.md` with a short section summarizing the job CLI usage; tell me if you want that and I'll make a small patch.
