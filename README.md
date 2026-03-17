# GBIF DNA Pipeline

A Spark-based pipeline for processing and cleaning DNA/RNA sequences, with support for GBIF vocabulary lookups.

## Modules

| Module | Description |
|--------|-------------|
| `gbif-dna-core` | Core sequence processing library |
| `gbif-dna-spark-udf` | Spark SQL UDFs for sequence processing and vocabulary lookup |
| `gbif-dna-spark` | Spark jobs for batch processing |

## Quick Start

### Build

```bash
mvn clean package
```

### Run Tests

```bash
mvn test
```

## Spark UDFs

### ProcessSequence UDF

Cleans and validates DNA/RNA sequences.

```java
import org.gbif.dna.spark.udf.ProcessSequenceUdf;

// Register the UDF
ProcessSequenceUdf.register(spark);

// Use in SQL
spark.sql("SELECT processSequence(dna_sequence, null) as result FROM sequences");
```

**With custom options:**
```sql
SELECT processSequence(dna_sequence, map('nrunCapFrom', '10', 'nrunCapTo', '5')) as result FROM sequences;
```

### Vocabulary Lookup UDF

Looks up terms against GBIF vocabularies.

```java
import org.gbif.vocabulary.spark.udf.GbifVocabularyLookupUdf;
import org.gbif.vocabulary.spark.udf.GbifVocabularyLookupUdf.VocabConfig;

// Broadcast config to executors
VocabConfig cfg = new VocabConfig("https://api.gbif.org/v1/vocabularies", "target_gene");
Broadcast<VocabConfig> bCfg = spark.sparkContext().broadcast(cfg, scala.reflect.ClassTag$.MODULE$.apply(VocabConfig.class));

// Register the UDF
GbifVocabularyLookupUdf.register(spark, "vocabLookup", bCfg);

// Use in SQL
spark.sql("SELECT vocabLookup(v_targetgene) AS targetgene FROM occurrences");
```

## Spark Jobs

### ProcessSequencesJob

Processes DNA sequences from an Iceberg source table.

```bash
spark-submit \
  --class org.gbif.dna.spark.jobs.ProcessSequencesJob \
  gbif-dna-spark-1.0-SNAPSHOT.jar \
  --source-table default.occurrence_sequences \
  --target-table default.processed_sequences
```

### LookupTargetGeneJob

Applies vocabulary lookup to a source table.

```bash
spark-submit \
  --class org.gbif.dna.spark.jobs.LookupTargetGeneJob \
  gbif-dna-spark-1.0-SNAPSHOT.jar \
  --source-table prod.occurrence \
  --target-table prod.test_vocab \
  --vocab-url https://api.gbif.org/v1/vocabularies \
  --vocab-name target_gene \
  --source-column v_targetgene \
  --result-column targetgene
```

## Sequence Processing Pipeline

1. **Whitespace/gap removal** - Removes gaps (`-`, `.`) and whitespace
2. **Natural language detection** - Flags sequences containing words like "UNMERGED"
3. **RNA to DNA conversion** - Converts U to T
4. **End trimming** - Trims non-anchor characters from sequence ends
5. **N-run capping** - Caps consecutive N runs (≥6) to 5
6. **Quality metrics** - Calculates GC content, IUPAC compliance, etc.
7. **MD5 hash** - Generates unique identifier for cleaned sequence

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `sequence` | String | Cleaned sequence (null if invalid) |
| `sequenceLength` | Integer | Length of cleaned sequence |
| `gcContent` | Double | GC content ratio |
| `nonIupacFraction` | Double | Fraction of non-IUPAC characters |
| `nonACGTNFraction` | Double | Fraction of non-ACGTN characters |
| `nFraction` | Double | Fraction of N characters |
| `nNrunsCapped` | Integer | Number of N-runs capped |
| `naturalLanguageDetected` | Boolean | Natural language detected |
| `endsTrimmed` | Boolean | Ends were trimmed |
| `gapsOrWhitespaceRemoved` | Boolean | Gaps/whitespace removed |
| `nucleotideSequenceID` | String | MD5 hash of cleaned sequence |
| `invalid` | Boolean | Sequence is invalid |

## Requirements

- Java 17+
- Apache Spark 3.5+
- Maven 3.8+

## License

Apache License 2.0

