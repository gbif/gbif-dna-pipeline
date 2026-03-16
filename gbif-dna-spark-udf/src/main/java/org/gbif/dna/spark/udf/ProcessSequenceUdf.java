/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.dna.spark.udf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dna.core.SequenceProcessor;
import org.gbif.dna.core.SequenceProcessor.Config;

import java.util.Map;

/**
 * Spark SQL UDF for processing DNA/RNA sequences.
 * Takes a sequence string and an optional map of configuration options,
 * returns a struct containing the cleaned sequence and various quality metrics.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * // Register the UDF
 * ProcessSequenceUdf.register(spark);
 *
 * // Use in SQL with default options
 * spark.sql("SELECT processSequence(dna_sequence, null) as result FROM ...");
 *
 * // Use in SQL with custom options
 * spark.sql("SELECT processSequence(dna_sequence, map('nrunCapFrom', '10', 'nrunCapTo', '5')) as result FROM ...");
 * }</pre>
 *
 * <h3>Options</h3>
 * The options map supports the following keys:
 * <ul>
 *   <li>{@code anchorChars} - Characters to use as anchors (default: "ACGTU")</li>
 *   <li>{@code anchorMinrun} - Minimum anchor run length (default: 8)</li>
 *   <li>{@code anchorStrict} - Strict anchor characters (default: "ACGTU")</li>
 *   <li>{@code gapRegex} - Regex for gap characters (default: "[-\\.]")</li>
 *   <li>{@code naturalLanguageRegex} - Regex for natural language detection (default: "UNMERGED")</li>
 *   <li>{@code iupacRna} - IUPAC RNA characters (default: "ACGTURYSWKMBDHVN")</li>
 *   <li>{@code iupacDna} - IUPAC DNA characters (default: "ACGTRYSWKMBDHVN")</li>
 *   <li>{@code nrunCapFrom} - N-run cap from value (default: 6)</li>
 *   <li>{@code nrunCapTo} - N-run cap to value (default: 5)</li>
 * </ul>
 */
public final class ProcessSequenceUdf {

  /**
   * Return type schema for the UDF
   */
  public static final StructType RETURN_TYPE = DataTypes.createStructType(
      java.util.Arrays.asList(
          DataTypes.createStructField("seqId", DataTypes.StringType, true),
          DataTypes.createStructField("rawSequence", DataTypes.StringType, true),
          DataTypes.createStructField("sequence", DataTypes.StringType, true),
          DataTypes.createStructField("sequenceLength", DataTypes.IntegerType, true),
          DataTypes.createStructField("nonIupacFraction", DataTypes.DoubleType, true),
          DataTypes.createStructField("nonACGTNFraction", DataTypes.DoubleType, true),
          DataTypes.createStructField("nFraction", DataTypes.DoubleType, true),
          DataTypes.createStructField("nNrunsCapped", DataTypes.IntegerType, true),
          DataTypes.createStructField("gcContent", DataTypes.DoubleType, true),
          DataTypes.createStructField("naturalLanguageDetected", DataTypes.BooleanType, true),
          DataTypes.createStructField("endsTrimmed", DataTypes.BooleanType, true),
          DataTypes.createStructField("gapsOrWhitespaceRemoved", DataTypes.BooleanType, true),
          DataTypes.createStructField("nucleotideSequenceID", DataTypes.StringType, true),
          DataTypes.createStructField("invalid", DataTypes.BooleanType, true)
      )
  );

  private ProcessSequenceUdf() {
    // utility class
  }

  /**
   * Register the processSequence UDF with the given Spark session.
   *
   * @param spark the Spark session
   */
  public static void register(SparkSession spark) {
    spark.udf().register(
        "processSequence",
        (UDF2<String, Map<String, String>, Row>) ProcessSequenceUdf::process,
        RETURN_TYPE
    );
  }

  // Per-thread processor cache: maps config to processor to avoid recreating processors for same config
  private static final ThreadLocal<ProcessorCache> processorCacheLocal = ThreadLocal.withInitial(ProcessorCache::new);

  /**
   * Process a DNA/RNA sequence with optional configuration options.
   *
   * @param dna  the raw DNA/RNA sequence
   * @param opts optional configuration map (may be null)
   * @return a Row containing the processing results
   */
  private static Row process(String dna, Map<String, String> opts) {
    if (dna == null) {
      return null;
    }

    Config config = buildConfig(opts);
    SequenceProcessor processor = processorCacheLocal.get().getOrCreate(config);
    SequenceProcessor.Result result = processor.processOneSequence(dna, null);

    return RowFactory.create(
        result.seqId(),
        result.rawSequence(),
        result.sequence(),
        result.sequenceLength(),
        result.nonIupacFraction(),
        result.nonACGTNFraction(),
        result.nFraction(),
        result.nNrunsCapped(),
        result.gcContent(),
        result.naturalLanguageDetected(),
        result.endsTrimmed(),
        result.gapsOrWhitespaceRemoved(),
        result.nucleotideSequenceID(),
        result.invalid()
    );
  }

  /**
   * Build a Config from the options map, using defaults for missing values.
   */
  private static Config buildConfig(Map<String, String> opts) {
    if (opts == null || opts.isEmpty()) {
      return Config.defaultConfig();
    }

    Config defaults = Config.defaultConfig();

    String anchorChars = opts.getOrDefault("anchorChars", defaults.anchorChars());
    int anchorMinrun = parseIntOrDefault(opts.get("anchorMinrun"), defaults.anchorMinrun());
    String anchorStrict = opts.getOrDefault("anchorStrict", defaults.anchorStrict());
    String gapRegex = opts.getOrDefault("gapRegex", defaults.gapRegex());
    String naturalLanguageRegex = opts.getOrDefault("naturalLanguageRegex", defaults.naturalLanguageRegex());
    String iupacRna = opts.getOrDefault("iupacRna", defaults.iupacRna());
    String iupacDna = opts.getOrDefault("iupacDna", defaults.iupacDna());
    int nrunCapFrom = parseIntOrDefault(opts.get("nrunCapFrom"), defaults.nrunCapFrom());
    int nrunCapTo = parseIntOrDefault(opts.get("nrunCapTo"), defaults.nrunCapTo());

    return new Config(
        anchorChars,
        anchorMinrun,
        anchorStrict,
        gapRegex,
        naturalLanguageRegex,
        iupacRna,
        iupacDna,
        nrunCapFrom,
        nrunCapTo
    );
  }

  private static int parseIntOrDefault(String value, int defaultValue) {
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * Simple cache for SequenceProcessor instances to avoid recreating them for the same config.
   */
  private static class ProcessorCache {
    private Config lastConfig;
    private SequenceProcessor lastProcessor;

    SequenceProcessor getOrCreate(Config config) {
      if (lastProcessor != null && config.equals(lastConfig)) {
        return lastProcessor;
      }
      lastConfig = config;
      lastProcessor = new SequenceProcessor(config);
      return lastProcessor;
    }
  }
}
