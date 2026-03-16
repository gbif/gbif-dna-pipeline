package org.gbif.dna.spark.jobs;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.vocabulary.spark.udf.GbifVocabularyLookupUdf;
import org.gbif.vocabulary.spark.udf.GbifVocabularyLookupUdf.VocabConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;


/**
 * Spark job that reads a source table containing `target_gene` raw values, applies the
 * GBIF vocabulary lookup UDF and writes the result table with fields:
 *   datasetkey, gbifid, raw_target_gene, target_gene
 */
@Command(
    name = "LookupTargetGeneJob",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description = "Spark job that applies GBIF vocabulary lookup UDF to a source table"
)
public class LookupTargetGeneJob implements Callable<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(LookupTargetGeneJob.class);

  @Option(names = {"-s", "--source-table"}, description = "Source Iceberg table", required = true)
  private String sourceTable;

  @Option(names = {"-t", "--target-table"}, description = "Target Iceberg table",  required = true)
  private String targetTable;

  @Option(names = {"-u", "--vocab-url"}, description = "GBIF Vocabulary API base URL (e.g., https://api.gbif.org/v1/vocabularies)",  required = true)
  private String vocabUrl;

  @Option(names = {"-n", "--vocab-name"}, description = "Vocabulary name",  required = true)
  private String vocabularyName;

  @Option(names = {"-c", "--source-column"}, description = "Source column name",  required = true)
  private String sourceColumn;

  @Option(names = {"-r", "--result-column"}, description = "Result column name",  required = true)
  private String resultColumn;

  public static void main(String[] args) {
    int exitCode = new CommandLine(new LookupTargetGeneJob()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    LOG.info("Source table: {}", sourceTable);
    LOG.info("Target table: {}", targetTable);
    LOG.info("Vocabulary URL: {}", vocabUrl);
    LOG.info("Vocabulary name: {}", vocabularyName);
    LOG.info("Source column: {}", sourceColumn);
    LOG.info("Result column: {}", resultColumn);

    SparkSession spark = SparkSession.builder()
        .appName("LookupTargetGeneJob")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .getOrCreate();

    // Broadcast a small, serializable vocab config. We avoid broadcasting the InMemoryVocabularyLookup
    // because it contains non-serializable cache2k internals. Executors will lazily build their own
    // InMemoryVocabularyLookup from this config on first use.
    Broadcast<VocabConfig> broadcastVocabConfig = null;

    try {

      VocabConfig cfg = new VocabConfig(vocabUrl, vocabularyName);
        // broadcast the small config to executors; ClassTag usage handled by Scala interop
      broadcastVocabConfig = spark.sparkContext().broadcast(cfg, scala.reflect.ClassTag$.MODULE$.apply(VocabConfig.class));
      LOG.info("Broadcasted vocab config for {}", vocabUrl);

      // Register the UDF with the broadcast variable
      GbifVocabularyLookupUdf.register(spark, "gbifVocabularyLookup", broadcastVocabConfig);

      // Verify source table exists to avoid an uncaught NoSuchTableException later.
      // Try spark.catalog().tableExists(fullName) first, then fall back to splitting dotted names
      // into (db, table) and calling tableExists(db, table). Strip surrounding backticks/quotes.
      boolean sourceExists = spark.catalog().tableExists(sourceTable);
      if (!sourceExists) {
        LOG.error("Source table or view '{}' cannot be found. Verify the spelling and correctness of the schema and catalog.", sourceTable);
        return ExitCode.ILLEGAL_STATE.code();
      }

      // Use the provided sourceColumn and resultColumn when composing the SQL
      String applySql = String.format(
          "SELECT datasetkey, gbifid, %s AS raw_%s, gbifVocabularyLookup(%s) AS %s FROM %s",
          sourceColumn, sourceColumn, sourceColumn, resultColumn, sourceTable
      );
      LOG.info("Running select: {}", applySql);
      Dataset<Row> df = spark.sql(applySql);

      // Persist result (write to Iceberg target table)
      LOG.info("Writing {} rows to target table {}", df.count(), targetTable);

      df.write()
        .format("iceberg")
        .mode(SaveMode.Overwrite)
        .saveAsTable(targetTable);

      LOG.info("Write complete");

      // Unpersist the broadcast once finished
      if (broadcastVocabConfig != null) {
        try {
          broadcastVocabConfig.unpersist(true);
        } catch (Throwable t) {
          LOG.warn("Failed to unpersist broadcast vocab config", t);
        }
      }

      return ExitCode.OK.code();

    } catch (Exception e) {
      LOG.error("Job failed", e);
      return ExitCode.JOB_FAILED.code();
    } finally {
      spark.stop();
    }
  }
}
