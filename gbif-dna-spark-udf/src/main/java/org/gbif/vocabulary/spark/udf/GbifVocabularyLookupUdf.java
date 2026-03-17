package org.gbif.vocabulary.spark.udf;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.vocabulary.lookup.InMemoryVocabularyLookup;
import org.gbif.vocabulary.lookup.LookupConcept;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Spark SQL UDF for looking up vocabulary terms using {@link InMemoryVocabularyLookup}.
 *
 * <p>The UDF uses a per-executor lazily initialized {@link InMemoryVocabularyLookup} built from a
 * broadcasted {@link VocabConfig}. This avoids serializing the lookup itself (which contains
 * non-serializable cache2k internals) and ensures each executor builds its own lookup instance
 * once on first use.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * // On the driver: broadcast a small serializable config
 * VocabConfig cfg = new VocabConfig("https://api.gbif.org/v1/vocabularies", "LifeStage");
 * Broadcast<VocabConfig> bCfg = spark.sparkContext().broadcast(cfg, scala.reflect.ClassTag$.MODULE$.apply(VocabConfig.class));
 *
 * // Register the UDF
 * GbifVocabularyLookupUdf.register(spark, "vocabLookup", bCfg);
 *
 * // Use in SQL
 * spark.sql("SELECT vocabLookup(raw_lifestage) AS lifestage FROM ...");
 * }</pre>
 */
public final class GbifVocabularyLookupUdf {

  private static final Logger LOG = LoggerFactory.getLogger(GbifVocabularyLookupUdf.class);

  private GbifVocabularyLookupUdf() {
    // utility class
  }

  /**
   * Register a vocabulary lookup UDF with the given Spark session.
   *
   * @param spark          the Spark session
   * @param udfName        the name to register the UDF under
   * @param broadcastConfig broadcast variable containing the vocabulary API URL and vocabulary name
   */
  public static void register(SparkSession spark, String udfName, Broadcast<VocabConfig> broadcastConfig) {
    spark.udf().register(udfName, fromBroadcast(broadcastConfig), DataTypes.StringType);
  }

  /**
   * Create a UDF that uses a broadcasted {@link VocabConfig} to lazily build an
   * {@link InMemoryVocabularyLookup} on each executor.
   *
   * @param broadcastConfig broadcast variable containing the vocabulary API URL and vocabulary name
   * @return a Spark UDF1 that maps raw terms to their canonical vocabulary concept name
   */
  public static UDF1<String, String> fromBroadcast(Broadcast<VocabConfig> broadcastConfig) {
    return new UDF1<>() {
      @Serial
      private static final long serialVersionUID = 1L;

      @Override
      public String call(String term) {
        if (term == null || broadcastConfig == null) return null;
        InMemoryVocabularyLookup lookup = getOrCreateExecutorLookup(broadcastConfig.value());
        if (lookup == null) return null;
        Optional<LookupConcept> concept = lookup.lookup(term);
        return concept.map(lc -> lc.getConcept().getName()).orElse(null);
      }
    };
  }

  // ------------------------------------------------------------------------------------------
  // UDF with lineage: returns STRUCT<concept: STRING, lineage: ARRAY<STRING>>
  // ------------------------------------------------------------------------------------------

  /**
   * Return type for the UDF that includes lineage:
   * STRUCT&lt;concept: STRING, lineage: ARRAY&lt;STRING&gt;&gt;
   */
  public static final StructType RETURN_TYPE_WITH_LINEAGE = DataTypes.createStructType(new StructField[]{
      DataTypes.createStructField("concept", DataTypes.StringType, true),
      DataTypes.createStructField("lineage", DataTypes.createArrayType(DataTypes.StringType), true)
  });

  /**
   * Register a vocabulary lookup UDF that returns both concept and lineage.
   *
   * @param spark          the Spark session
   * @param udfName        the name to register the UDF under
   * @param broadcastConfig broadcast variable containing the vocabulary API URL and vocabulary name
   */
  public static void registerWithLineage(SparkSession spark, String udfName, Broadcast<VocabConfig> broadcastConfig) {
    spark.udf().register(udfName, fromBroadcastWithLineage(broadcastConfig), RETURN_TYPE_WITH_LINEAGE);
  }

  /**
   * Create a UDF that uses a broadcasted {@link VocabConfig} to lazily build an
   * {@link InMemoryVocabularyLookup} on each executor.
   *
   * <p>Returns a struct with the concept name and its lineage (list of parent concept names).
   *
   * @param broadcastConfig broadcast variable containing the vocabulary API URL and vocabulary name
   * @return a Spark UDF1 that maps raw terms to a struct with concept name and lineage
   */
  public static UDF1<String, Row> fromBroadcastWithLineage(Broadcast<VocabConfig> broadcastConfig) {
    return new UDF1<>() {
      @Serial
      private static final long serialVersionUID = 1L;

      @Override
      public Row call(String term) {
        if (term == null || broadcastConfig == null) return null;
        InMemoryVocabularyLookup lookup = getOrCreateExecutorLookup(broadcastConfig.value());
        if (lookup == null) return null;
        Optional<LookupConcept> lookupConcept = lookup.lookup(term);
        if (lookupConcept.isEmpty()) return null;

        LookupConcept lc = lookupConcept.get();
        String conceptName = lc.getConcept().getName();
        List<String> lineage = lc.getParents() != null
            ? lc.getParents().stream().map(LookupConcept.Parent::getName).toList()
            : List.of();

        return RowFactory.create(conceptName, lineage.toArray(new String[0]));
      }
    };
  }


  // ------------------------------------------------------------------------------------------
  // Executor-local lookup instance. Built on executor JVMs only.
  // ------------------------------------------------------------------------------------------

  private static InMemoryVocabularyLookup executorLookup;
  private static VocabConfig executorConfig;

  private static synchronized InMemoryVocabularyLookup getOrCreateExecutorLookup(VocabConfig cfg) {
    if (cfg == null) return null;
    // Return existing lookup if config matches
    if (executorLookup != null && cfg.equals(executorConfig)) {
      return executorLookup;
    }
    try {
      executorLookup = InMemoryVocabularyLookup.newBuilder()
          .from(cfg.url(), cfg.vocabularyName())
          .build();
      executorConfig = cfg;
      LOG.info("Executor built InMemoryVocabularyLookup for vocabulary '{}' from {}", cfg.vocabularyName(), cfg.url());
    } catch (Throwable t) {
      LOG.warn("Executor failed to build InMemoryVocabularyLookup for vocabulary '{}' from {}", cfg.vocabularyName(), cfg.url(), t);
      executorLookup = null;
      executorConfig = null;
    }
    return executorLookup;
  }

  // ------------------------------------------------------------------------------------------
  // Serializable config holder (safe to broadcast)
  // ------------------------------------------------------------------------------------------

  /**
   * Serializable configuration for building an {@link InMemoryVocabularyLookup} on executors.
   */
  public record VocabConfig(String url, String vocabularyName) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Create a new vocabulary configuration.
     *
     * @param url            the vocabulary API base URL (e.g.,
     *                       "https://api.gbif.org/v1/vocabularies")
     * @param vocabularyName the name of the vocabulary to load (e.g., "LifeStage", "target_gene")
     */
    public VocabConfig {}
  }
}
