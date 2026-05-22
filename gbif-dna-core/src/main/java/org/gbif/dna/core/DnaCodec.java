package org.gbif.dna.core;

import com.github.luben.zstd.Zstd;
import java.util.Base64;

public class DnaCodec {

  private static final byte[] IUPAC_ENCODE = new byte[256];
  private static final char[] IUPAC_DECODE = {
      'A', 'C', 'G', 'T', 'U', 'R', 'Y', 'S',
      'W', 'K', 'M', 'B', 'D', 'H', 'V', 'N'
  };

  static {
    java.util.Arrays.fill(IUPAC_ENCODE, (byte) 15);
    IUPAC_ENCODE['A'] = IUPAC_ENCODE['a'] = 0;
    IUPAC_ENCODE['C'] = IUPAC_ENCODE['c'] = 1;
    IUPAC_ENCODE['G'] = IUPAC_ENCODE['g'] = 2;
    IUPAC_ENCODE['T'] = IUPAC_ENCODE['t'] = 3;
    IUPAC_ENCODE['U'] = IUPAC_ENCODE['u'] = 4;
    IUPAC_ENCODE['R'] = IUPAC_ENCODE['r'] = 5;
    IUPAC_ENCODE['Y'] = IUPAC_ENCODE['y'] = 6;
    IUPAC_ENCODE['S'] = IUPAC_ENCODE['s'] = 7;
    IUPAC_ENCODE['W'] = IUPAC_ENCODE['w'] = 8;
    IUPAC_ENCODE['K'] = IUPAC_ENCODE['k'] = 9;
    IUPAC_ENCODE['M'] = IUPAC_ENCODE['m'] = 10;
    IUPAC_ENCODE['B'] = IUPAC_ENCODE['b'] = 11;
    IUPAC_ENCODE['D'] = IUPAC_ENCODE['d'] = 12;
    IUPAC_ENCODE['H'] = IUPAC_ENCODE['h'] = 13;
    IUPAC_ENCODE['V'] = IUPAC_ENCODE['v'] = 14;
    IUPAC_ENCODE['N'] = IUPAC_ENCODE['n'] = 15;
    IUPAC_ENCODE['-'] = 15;
  }

  // Binary layout: [0..3] original length (int), [4..] zstd compressed nibble-packed bytes
  public static String encode(String sequence) {
    byte[] packed     = nibblePack(sequence);
    byte[] compressed = Zstd.compress(packed, 3);

    int    len    = sequence.length();
    byte[] result = new byte[4 + compressed.length];
    result[0] = (byte)(len >> 24);
    result[1] = (byte)(len >> 16);
    result[2] = (byte)(len >> 8);
    result[3] = (byte)(len);
    System.arraycopy(compressed, 0, result, 4, compressed.length);

    return Base64.getEncoder().encodeToString(result);
  }

  public static String decode(String encoded) {
    byte[] raw = Base64.getDecoder().decode(encoded);

    int originalLen = ((raw[0] & 0xFF) << 24)
        | ((raw[1] & 0xFF) << 16)
        | ((raw[2] & 0xFF) << 8)
        |  (raw[3] & 0xFF);

    byte[] compressed = new byte[raw.length - 4];
    System.arraycopy(raw, 4, compressed, 0, compressed.length);

    int    packedLen = (originalLen + 1) / 2;
    byte[] packed    = new byte[packedLen];
    long   result    = Zstd.decompress(packed, compressed);

    if (Zstd.isError(result)) {
      throw new IllegalStateException(
          "Zstd decompression failed: " + Zstd.getErrorName(result));
    }

    return nibbleUnpack(packed, originalLen);
  }

  private static byte[] nibblePack(String sequence) {
    int    len       = sequence.length();
    byte[] packed    = new byte[(len + 1) / 2];
    for (int i = 0; i < len; i++) {
      byte nibble = IUPAC_ENCODE[sequence.charAt(i)];
      if ((i & 1) == 0) packed[i / 2]  = (byte)(nibble << 4);
      else               packed[i / 2] |= nibble;
    }
    return packed;
  }

  private static String nibbleUnpack(byte[] packed, int originalLen) {
    char[] seq = new char[originalLen];
    for (int i = 0; i < originalLen; i++) {
      int nibble = (i & 1) == 0
          ? (packed[i / 2] >> 4) & 0x0F
          :  packed[i / 2]       & 0x0F;
      seq[i] = IUPAC_DECODE[nibble];
    }
    return new String(seq);
  }
}