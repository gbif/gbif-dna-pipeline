package org.gbif.vocabulary.udf;

import static org.junit.jupiter.api.Assertions.*;

import org.gbif.vocabulary.spark.udf.GbifVocabularyLookupUdf.VocabConfig;
import org.junit.jupiter.api.Test;

import java.io.*;

public class GbifVocabularyLookupUdfTest {

  @Test
  public void testVocabConfigIsSerializable() throws Exception {
    VocabConfig cfg = new VocabConfig("https://api.gbif.org/v1/vocabularies", "LifeStage");

    // Serialize
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(cfg);
    }

    // Deserialize
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    VocabConfig restored;
    try (ObjectInputStream ois = new ObjectInputStream(bis)) {
      restored = (VocabConfig) ois.readObject();
    }

    assertEquals("https://api.gbif.org/v1/vocabularies", restored.url());
    assertEquals("LifeStage", restored.vocabularyName());
  }

  @Test
  public void testVocabConfigGetters() {
    VocabConfig cfg = new VocabConfig("http://example.com/vocab", "target_gene");
    assertEquals("http://example.com/vocab", cfg.url());
    assertEquals("target_gene", cfg.vocabularyName());
  }
}
