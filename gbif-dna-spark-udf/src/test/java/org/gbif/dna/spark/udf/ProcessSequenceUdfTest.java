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
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ProcessSequenceUdf}.
 *
 * These tests call the private process method via reflection to avoid needing
 * a full Spark session.
 */
public class ProcessSequenceUdfTest {

  /**
   * Call the private process method via reflection.
   */
  private Row callUdf(String sequence, Map<String, String> opts) throws Exception {
    Method processMethod = ProcessSequenceUdf.class.getDeclaredMethod("process", String.class, Map.class);
    processMethod.setAccessible(true);
    return (Row) processMethod.invoke(null, sequence, opts);
  }

  /**
   * Convenience method for calling with default options.
   */
  private Row callUdf(String sequence) throws Exception {
    return callUdf(sequence, null);
  }

  @Test
  public void testNullSequenceReturnsNull() throws Exception {
    Row result = callUdf(null);
    assertNull(result);
  }

  @Test
  public void testBasicSequenceWithDefaults() throws Exception {
    String sequence = "ACGTACGTACGT";
    Row result = callUdf(sequence);

    assertNotNull(result);
    // seqId is null when not provided
    assertNull(result.get(0));
    assertEquals(sequence, result.getString(1)); // rawSequence
    assertEquals(sequence, result.getString(2)); // sequence (cleaned)
    assertEquals(12, result.getInt(3)); // sequenceLength
    assertEquals(0.0, result.getDouble(4), 1e-9); // nonIupacFraction
    assertEquals(0.0, result.getDouble(5), 1e-9); // nonACGTNFraction
    assertFalse(result.getBoolean(9)); // naturalLanguageDetected
    assertFalse(result.getBoolean(10)); // endsTrimmed
    assertFalse(result.getBoolean(11)); // gapsOrWhitespaceRemoved
    assertNotNull(result.getString(12)); // nucleotideSequenceID (MD5)
    assertFalse(result.getBoolean(13)); // invalid
  }

  @Test
  public void testSequenceWithWhitespace() throws Exception {
    String sequence = "ACGT ACGT ACGT";
    Row result = callUdf(sequence);

    assertNotNull(result);
    assertEquals("ACGTACGTACGT", result.getString(2)); // sequence (cleaned)
    assertTrue(result.getBoolean(11)); // gapsOrWhitespaceRemoved
    assertFalse(result.getBoolean(13)); // invalid
  }

  @Test
  public void testSequenceWithGaps() throws Exception {
    String sequence = "ACGT-ACGT..ACGT";
    Row result = callUdf(sequence);

    assertNotNull(result);
    assertEquals("ACGTACGTACGT", result.getString(2)); // sequence (cleaned)
    assertTrue(result.getBoolean(11)); // gapsOrWhitespaceRemoved
    assertFalse(result.getBoolean(13)); // invalid
  }

  @Test
  public void testRnaToDonaConversion() throws Exception {
    String sequence = "ACGUACGUACGU";
    Row result = callUdf(sequence);

    assertNotNull(result);
    assertEquals("ACGTACGTACGT", result.getString(2)); // U converted to T
    assertFalse(result.getBoolean(13)); // invalid
  }

  @Test
  public void testLowercaseToUppercase() throws Exception {
    String sequence = "acgtacgtacgt";
    Row result = callUdf(sequence);

    assertNotNull(result);
    assertEquals("ACGTACGTACGT", result.getString(2)); // sequence uppercased
    assertFalse(result.getBoolean(13)); // invalid
  }

  @Test
  public void testNaturalLanguageDetection() throws Exception {
    // Default naturalLanguageRegex is "UNMERGED"
    String sequence = "ACGTUNMERGEDACGT";
    Row result = callUdf(sequence);

    assertNotNull(result);
    assertTrue(result.getBoolean(9)); // naturalLanguageDetected
    assertTrue(result.getBoolean(13)); // invalid
  }

  @Test
  public void testNrunCapping() throws Exception {
    // Default nrunCapFrom=6, nrunCapTo=5, meaning N-runs of 6+ are capped to 5
    String sequence = "ACGTACGTNNNNNNNNNNACGTACGT";
    Row result = callUdf(sequence);

    assertNotNull(result);
    String cleanedSequence = result.getString(2);
    // N-run of 10 should be capped to 5
    assertTrue(cleanedSequence.contains("NNNNN"));
    assertFalse(cleanedSequence.contains("NNNNNN")); // Should not have 6 consecutive Ns
    assertEquals(1, result.getInt(7)); // nNrunsCapped
    assertFalse(result.getBoolean(13)); // invalid
  }

  @Test
  public void testCustomNrunCap() throws Exception {
    // Test with custom options
    Map<String, String> opts = new HashMap<>();
    opts.put("nrunCapFrom", "3");
    opts.put("nrunCapTo", "2");

    String sequence = "ACGTACGTNNNNACGTACGT";
    Row result = callUdf(sequence, opts);

    assertNotNull(result);
    String cleanedSequence = result.getString(2);
    // N-run of 4 should be capped to 2
    assertTrue(cleanedSequence.contains("NN"));
    assertFalse(cleanedSequence.contains("NNN")); // Should not have 3 consecutive Ns
    assertEquals(1, result.getInt(7)); // nNrunsCapped
  }

  @Test
  public void testCustomAnchorChars() throws Exception {
    // Test sequence trimming with custom options
    Map<String, String> opts = new HashMap<>();
    opts.put("anchorChars", "ACGT");
    opts.put("anchorMinrun", "4");

    String sequence = "XXXXACGTACGTACGTXXXX";
    Row result = callUdf(sequence, opts);

    assertNotNull(result);
    assertEquals("ACGTACGTACGT", result.getString(2)); // Trimmed to anchor region
    assertTrue(result.getBoolean(10)); // endsTrimmed
  }

  @Test
  public void testCustomGapRegex() throws Exception {
    // Test with custom gap regex that includes underscore
    Map<String, String> opts = new HashMap<>();
    opts.put("gapRegex", "[-\\._]");

    String sequence = "ACGT_ACGT_ACGT";
    Row result = callUdf(sequence, opts);

    assertNotNull(result);
    assertEquals("ACGTACGTACGT", result.getString(2)); // Underscores removed
    assertTrue(result.getBoolean(11)); // gapsOrWhitespaceRemoved
  }

  @Test
  public void testGcContent() throws Exception {
    // Sequence with known GC content: 6 GC out of 12 ACGT = 50%
    String sequence = "GCGCGCATATAT";
    Row result = callUdf(sequence);

    assertNotNull(result);
    assertEquals(0.5, result.getDouble(8), 0.001); // gcContent = 50%
  }

  @Test
  public void testNonIupacCharactersMarkInvalid() throws Exception {
    // Sequence with non-IUPAC characters (X is not in default IUPAC DNA)
    // Need 8+ consecutive anchor chars for the sequence not to be wiped
    String sequence = "ACGTACGTACGTXACGTACGTACGT";
    Row result = callUdf(sequence);

    assertNotNull(result);
    assertTrue(result.getDouble(4) > 0); // nonIupacFraction > 0
    assertTrue(result.getBoolean(13)); // invalid
    assertNull(result.get(2)); // sequence is null when invalid
    assertNull(result.get(12)); // nucleotideSequenceID is null when invalid
  }

  @Test
  public void testResultSchemaNotNull() throws Exception {
    Row result = callUdf("ACGTACGT");
    assertNotNull(result);
    assertEquals(14, result.length());
  }

  @Test
  public void testEmptySequence() throws Exception {
    String sequence = "";
    Row result = callUdf(sequence);

    assertNotNull(result);
    assertEquals(0, result.getInt(3)); // sequenceLength
  }

  @Test
  public void testQuestionMarkToN() throws Exception {
    // Need 8+ consecutive anchor chars for the sequence not to be wiped
    String sequence = "ACGTACGTACGT?ACGTACGTACGT";
    Row result = callUdf(sequence);

    assertNotNull(result);
    assertTrue(result.getString(2).contains("N")); // ? converted to N
    assertFalse(result.getString(2).contains("?")); // No ? in result
  }

  @Test
  public void testAllConfigParamsProvided() throws Exception {
    // Test with all config params provided
    Map<String, String> opts = new HashMap<>();
    opts.put("anchorChars", "ACGTU");
    opts.put("anchorMinrun", "8");
    opts.put("anchorStrict", "ACGTU");
    opts.put("gapRegex", "[-\\.]");
    opts.put("naturalLanguageRegex", "UNMERGED");
    opts.put("iupacRna", "ACGTURYSWKMBDHVN");
    opts.put("iupacDna", "ACGTRYSWKMBDHVN");
    opts.put("nrunCapFrom", "6");
    opts.put("nrunCapTo", "5");

    String sequence = "acgt-acgt";
    Row result = callUdf(sequence, opts);

    assertNotNull(result);
    assertEquals("ACGTACGT", result.getString(2)); // cleaned sequence
    assertFalse(result.getBoolean(13)); // invalid
  }

  @Test
  public void testReturnTypeSchema() {
    assertNotNull(ProcessSequenceUdf.RETURN_TYPE);
    assertEquals(14, ProcessSequenceUdf.RETURN_TYPE.fields().length);
    assertEquals("seqId", ProcessSequenceUdf.RETURN_TYPE.fields()[0].name());
    assertEquals("rawSequence", ProcessSequenceUdf.RETURN_TYPE.fields()[1].name());
    assertEquals("sequence", ProcessSequenceUdf.RETURN_TYPE.fields()[2].name());
    assertEquals("sequenceLength", ProcessSequenceUdf.RETURN_TYPE.fields()[3].name());
    assertEquals("invalid", ProcessSequenceUdf.RETURN_TYPE.fields()[13].name());
  }
}
