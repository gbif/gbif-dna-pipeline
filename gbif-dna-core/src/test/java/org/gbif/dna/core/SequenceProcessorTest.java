package org.gbif.dna.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class SequenceProcessorTest {

  private SequenceProcessor.Config permissiveConfig() {
    return new SequenceProcessor.Config(
      "ACGTU",
      1, // anchor min run 1 so sequences are not wiped during tests
      "ACGTU",
      "[-\\.]",
      "UNMERGED",
      "ACGTURYSWKMBDHVN",
      "ACGTRYSWKMBDHVN",
      6,
      5
    );
  }

  @Test
  public void testWhitespaceNormalizationAndUppercase() {
    SequenceProcessor sp = new SequenceProcessor(permissiveConfig());
    SequenceProcessor.Result r = sp.processOneSequence("acgt acgt\tacgt\nacgt", "id1");
    assertFalse(r.invalid());
    assertEquals(16, r.sequenceLength());
    assertEquals("acgtacgtacgtacgt".toUpperCase(), r.sequence());
  }

  @Test
  public void testGapRemoval() {
    SequenceProcessor sp = new SequenceProcessor(permissiveConfig());
    SequenceProcessor.Result r = sp.processOneSequence("ACGT-ACGT..ACGT", "id2");
    assertFalse(r.invalid());
    assertEquals("ACGTACGTACGT", r.sequence());
    assertTrue(r.gapsOrWhitespaceRemoved());
  }

  @Test
  public void testRnaToDnaConversionAndQuestionToN() {
    SequenceProcessor sp = new SequenceProcessor(permissiveConfig());
    SequenceProcessor.Result r = sp.processOneSequence("ACGTU?ACGTU", "id3");
    assertFalse(r.invalid());
    assertEquals("ACGTTNACGTT", r.sequence());
  }

  @Test
  public void testNRunCapping() {
    SequenceProcessor.Config cfg = permissiveConfig();
    SequenceProcessor sp = new SequenceProcessor(cfg);

    // 10 Ns should be capped to 5
    SequenceProcessor.Result r = sp.processOneSequence("ACGTNNNNNNNNNNACGT", "id4");
    assertFalse(r.invalid());
    assertEquals("ACGTNNNNNACGT", r.sequence());
    assertEquals(1, r.nNrunsCapped());
  }

  @Test
  public void testTrimToAnchorsWipeWhenMissing() {
    SequenceProcessor sp = new SequenceProcessor();
    // no long anchor run present -> SequenceProcessor currently trims to empty string (wipes)
    SequenceProcessor.Result r = sp.processOneSequence("THISISNOTDNASEQ", "id5");
    // according to implementation, wiped sequence becomes empty string and invalid remains false
    assertFalse(r.invalid());
    assertEquals(0, r.sequenceLength());
    assertEquals("", r.sequence());
  }

  @Test
  public void testMd5AndNucleotideSequenceID() {
    SequenceProcessor sp = new SequenceProcessor(permissiveConfig());
    SequenceProcessor.Result r = sp.processOneSequence("ACGTACGTACGTACGT", "id6");
    assertFalse(r.invalid());
    assertNotNull(r.nucleotideSequenceID());
    assertEquals(32, r.nucleotideSequenceID().length());
  }
}
