package org.gbif.dna.spark.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.dna.spark.udf.ProcessSequenceUdf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

/**
 * Spark job to read an Iceberg source table of occurrence sequences, apply the
 * `processSequence` UDF and write the cleaned result into a new Iceberg table.
 *
 * Usage:
 *   spark-submit --class org.gbif.dna.spark.jobs.ProcessSequencesJob app.jar \
 *     --source-table catalog.db.occurrence_ext_gbif_dnaderiveddata \
 *     --target-table catalog.db.occurrence_nucleotide_sequence
 */
@Command(
    name = "ProcessSequencesJob",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description = "Spark job to process DNA sequences using the processSequence UDF"
)
public class ProcessSequencesJob implements Callable<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessSequencesJob.class);

  @Option(names = {"-s", "--source-table"}, description = "Source Iceberg table (e.g: default.occurrence_ext_gbif_dnaderiveddata)", required = true)
  private String sourceTable;

  @Option(names = {"-t", "--target-table"}, description = "Target Iceberg table (e.g: default.occurrence_nucleotide_sequences)", required = true)
  private String targetTable;

  public static void main(String[] args) {
    int exitCode = new CommandLine(new ProcessSequencesJob()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    LOG.info("Source table: {}", sourceTable);
    LOG.info("Target table: {}", targetTable);

    SparkSession spark = SparkSession.builder()
        .appName("ProcessSequencesJob")
        // Iceberg extensions - adjust according to your environment/catalog
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        // Example: use the Spark (Hive) catalog backed by Hive Metastore for Iceberg tables
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .getOrCreate();

    try {
      // Register the Spark UDF
      ProcessSequenceUdf.register(spark);

      // Step 1: select and apply the UDF producing a struct column `result`
      String applySql = "SELECT datasetkey, gbifid, processSequence(v_dnasequence, null) as result FROM " + sourceTable;
      LOG.info("Running select: {}", applySql);
      Dataset<Row> df = spark.sql(applySql);

      // Step 2: extract fields from the struct and write to Iceberg (or cataloged table)
      String expandSql = """
          SELECT
            datasetkey,
            gbifid,
            result.sequence as cleaned_sequence,
            result.sequenceLength as sequenceLength,
            result.nonIupacFraction as nonIupacFraction,
            result.nonACGTNFraction as nonACGTNFraction,
            result.nFraction as nFraction,
            result.nNrunsCapped as nNrunsCapped,
            result.gcContent as gcContent,
            result.naturalLanguageDetected as naturalLanguageDetected,
            result.endsTrimmed as endsTrimmed,
            result.gapsOrWhitespaceRemoved as gapsOrWhitespaceRemoved,
            result.nucleotideSequenceID as nucleotideSequenceID,
            result.invalid as invalid
          FROM tmp_processed""";

      df.createOrReplaceTempView("tmp_processed");
      Dataset<Row> finalDf = spark.sql(expandSql);

      LOG.info("Writing {} rows to target table {}", finalDf.count(), targetTable);

      // Write the result to the Iceberg target table. The exact write mode and catalog depend on your environment.
      finalDf.write()
          .format("iceberg")
          .mode(SaveMode.Overwrite)
          .saveAsTable(targetTable);

      LOG.info("Write complete");

      return ExitCode.OK.code();

    } catch (Exception e) {
      LOG.error("Job failed", e);
      return ExitCode.JOB_FAILED.code();
    } finally {
      spark.stop();
    }
  }
}
