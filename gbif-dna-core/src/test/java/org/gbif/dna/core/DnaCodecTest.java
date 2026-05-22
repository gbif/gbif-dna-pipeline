package org.gbif.dna.core;

import com.github.luben.zstd.ZstdException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Base64;
import org.junit.jupiter.api.Test;

public class DnaCodecTest {

  @Test
  public void testRoundTripMixedIupacAndRnaCharacters() {
    String sequence = "ACGTURYSWKMBDHVN";
    String encoded = DnaCodec.encode(sequence);
    assertEquals(sequence, DnaCodec.decode(encoded));
  }

  @Test
  public void testRoundTripOddLengthSequence() {
    String sequence = "ACGTN";
    String encoded = DnaCodec.encode(sequence);

    assertEquals(sequence, DnaCodec.decode(encoded));
  }

  @Test
  public void testLowercaseAndUnknownCharactersNormalizeToUppercaseAndN() {
    String encoded = DnaCodec.encode("acgtu?x-");
    assertEquals("ACGTUNNN", DnaCodec.decode(encoded));
  }

  @Test
  public void testDecodeInvalidBase64Throws() {
    assertThrows(IllegalArgumentException.class, () -> DnaCodec.decode("not-base64!!"));
  }

  @Test
  public void testDecodeCorruptedPayloadThrowsRuntimeFailure() {
    String encoded = DnaCodec.encode("ACGTACGTACGT");
    byte[] raw = Base64.getDecoder().decode(encoded);

    // Corrupt compressed data payload to force zstd decompression failure.
    for (int i = 5; i < raw.length; i++) {
      raw[i] = (byte) ~raw[i];
    }

    String corrupted = Base64.getEncoder().encodeToString(raw);
    RuntimeException ex = assertThrows(RuntimeException.class, () -> DnaCodec.decode(corrupted));
    assertTrue(ex instanceof IllegalStateException || ex instanceof ZstdException);
  }
}
