# Nucleotide Sequence Processor

Spark UDF for cleaning and validating DNA/RNA sequences.

## Usage

### Java Registration

```java
import org.apache.spark.sql.SparkSession;
import org.gbif.dna.spark.udf.ProcessSequenceUdf;

SparkSession spark = SparkSession.builder().appName("example").getOrCreate();

// Register the UDF
ProcessSequenceUdf.register(spark);

// Use in SQL with default options
spark.sql("SELECT processSequence(dna_sequence, null) as result FROM sequences");

// Use with custom options
spark.sql("SELECT processSequence(dna_sequence, map('nrunCapFrom', '10', 'nrunCapTo', '5')) as result FROM sequences");
```

### SQL Example

```sql
-- Create nucleotide sequence table
CREATE TABLE prod.occurrence_nucleotide_sequence AS
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
        processSequence(dnasequence, null) as result
    FROM occurrence_ext_gbif_dnaderiveddata
    WHERE dnasequence IS NOT NULL
) t;
```

## Processing Pipeline

1. **Whitespace/gap removal** - Removes gaps (`-`, `.`) and whitespace
2. **Natural language detection** - Flags sequences containing words like "UNMERGED"
3. **End trimming** - Trims non-anchor characters from sequence ends
4. **N-run capping** - Caps consecutive N runs (≥6) to 5
5. **Quality metrics** - Calculates GC content, IUPAC compliance, etc.
6. **MD5 hash** - Generates unique identifier for cleaned sequence

## Output Columns

| Column | Type | Description |
|--------|------|-------------|
| `seqId` | String | Sequence ID (if provided) |
| `rawSequence` | String | Original input sequence |
| `sequence` | String | Cleaned sequence (null if invalid) |
| `sequenceLength` | Integer | Length of cleaned sequence |
| `nonIupacFraction` | Double | Fraction of non-IUPAC characters |
| `nonACGTNFraction` | Double | Fraction of non-ACGTN characters |
| `nFraction` | Double | Fraction of N characters |
| `nNrunsCapped` | Integer | Number of N-runs capped |
| `gcContent` | Double | GC content ratio |
| `naturalLanguageDetected` | Boolean | Natural language detected |
| `endsTrimmed` | Boolean | Ends were trimmed |
| `gapsOrWhitespaceRemoved` | Boolean | Gaps/whitespace removed |
| `nucleotideSequenceID` | String | MD5 hash of cleaned sequence |
| `invalid` | Boolean | Sequence is invalid |

## Configuration Options

The options map supports the following keys:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `anchorChars` | String | "ACGTU" | Characters to use as anchors |
| `anchorMinrun` | Integer | 8 | Minimum anchor run length |
| `anchorStrict` | String | "ACGTU" | Strict anchor characters |
| `gapRegex` | String | `"[-\\.]"` | Regex for gap characters |
| `naturalLanguageRegex` | String | "UNMERGED" | Regex for natural language detection |
| `iupacRna` | String | "ACGTURYSWKMBDHVN" | IUPAC RNA characters |
| `iupacDna` | String | "ACGTRYSWKMBDHVN" | IUPAC DNA characters |
| `nrunCapFrom` | Integer | 6 | N-run cap from value |
| `nrunCapTo` | Integer | 5 | N-run cap to value |

### Example with Custom Options

```sql
SELECT processSequence(
    dna_sequence,
    map(
        'anchorMinrun', '4',
        'nrunCapFrom', '10',
        'nrunCapTo', '5',
        'gapRegex', '[-\\._]'
    )
) as result
FROM sequences;
```

## Vocabulary Lookup UDF

The `GbifVocabularyLookupUdf` provides vocabulary term lookup functionality using GBIF's vocabulary API.

### Java Usage

```java
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.gbif.vocabulary.spark.udf.GbifVocabularyLookupUdf;
import org.gbif.vocabulary.spark.udf.GbifVocabularyLookupUdf.VocabConfig;

SparkSession spark = SparkSession.builder().appName("vocab-lookup").getOrCreate();

// Create and broadcast the vocabulary config
VocabConfig cfg = new VocabConfig("https://api.gbif.org/v1/vocabularies", "LifeStage");
Broadcast<VocabConfig> broadcastConfig = spark.sparkContext().broadcast(
    cfg,
    scala.reflect.ClassTag$.MODULE$.apply(VocabConfig.class)
);

// Register the UDF
GbifVocabularyLookupUdf.register(spark, "vocabLookup", broadcastConfig);

// Use in SQL
spark.sql("SELECT vocabLookup(raw_lifestage) AS lifestage FROM occurrences");

// Don't forget to unpersist when done
broadcastConfig.unpersist(true);
```

## Testing in Spark on Kubernetes

### Prerequisites

1. A running Kubernetes cluster with Spark operator or spark-submit capability
2. Access to a container registry (e.g., Docker Hub, GCR, ECR)
3. kubectl configured to access your cluster

### Step 1: Build the JAR

```bash
mvn clean package -DskipTests
```

### Step 2: Upload JAR to accessible storage

The JAR needs to be accessible from your Spark pods. Options include:

**Option A: Include in custom Spark image**
```dockerfile
FROM apache/spark:3.5.0
COPY target/gbif-dna-spark-udf-*.jar /opt/spark/jars/
```

**Option B: Upload to cloud storage (S3, GCS, HDFS)**
```bash
# For HDFS
hdfs dfs -put target/gbif-dna-spark-udf-*.jar /user/spark/jars/
```

### Step 3: Submit Spark job

```bash
spark-submit \
  --master k8s://https://<k8s-api-server>:6443 \
  --deploy-mode cluster \
  --name test-udf \
  --conf spark.kubernetes.container.image=apache/spark:3.5.0 \
  --jars hdfs://<namenode>:8020/user/spark/jars/gbif-dna-spark-udf-1.0-SNAPSHOT.jar \
  --class org.gbif.dna.spark.jobs.ProcessSequencesJob \
  hdfs://<namenode>:8020/user/spark/apps/gbif-dna-spark-1.0-SNAPSHOT.jar \
  --source-table default.occurrence_sequences \
  --target-table default.processed_sequences
```

### Using with Spark SQL

```java
import org.apache.spark.sql.SparkSession;
import org.gbif.dna.spark.udf.ProcessSequenceUdf;
import java.util.Arrays;

SparkSession spark = SparkSession.builder().appName("example").getOrCreate();

// Register the UDF
ProcessSequenceUdf.register(spark);

// Create sample data
var testData = spark.createDataFrame(Arrays.asList(
    org.apache.spark.sql.RowFactory.create("ACGTACGTACGTACGT"),
    org.apache.spark.sql.RowFactory.create("acgt-acgt-acgt-acgt"),
    org.apache.spark.sql.RowFactory.create("ACGUACGUACGUACGU"),
    org.apache.spark.sql.RowFactory.create("ACGTNNNNNNNNNNACGT")
), new org.apache.spark.sql.types.StructType(new org.apache.spark.sql.types.StructField[] {
    new org.apache.spark.sql.types.StructField("sequence",
        org.apache.spark.sql.types.DataTypes.StringType, false,
        org.apache.spark.sql.types.Metadata.empty())
}));

testData.createOrReplaceTempView("sequences");

// Process sequences with default config
var result = spark.sql("""
    SELECT
        result.rawSequence,
        result.sequence as cleaned_sequence,
        result.sequenceLength,
        result.gcContent,
        result.invalid
    FROM (
        SELECT processSequence(sequence, null) as result
        FROM sequences
    ) t
""");

result.show(false);
```

### Troubleshooting

**JAR not found:**
- Ensure the JAR path is correct and accessible from both driver and executors
- Check that the JAR is included in the container image or mounted correctly

**Class not found:**
- Verify the JAR contains all required dependencies
- Check `spark.driver.extraClassPath` and `spark.executor.extraClassPath` configs

**View logs:**
```bash
# Driver logs
kubectl logs <driver-pod-name>

# Executor logs
kubectl logs <executor-pod-name>
```

## Running Unit Tests

```bash
mvn test
```

To run a specific test class:
```bash
mvn test -Dtest=ProcessSequenceUdfTest
```
