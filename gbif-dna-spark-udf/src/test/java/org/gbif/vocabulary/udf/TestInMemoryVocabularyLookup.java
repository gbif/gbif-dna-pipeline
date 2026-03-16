package org.gbif.vocabulary.udf;

import java.util.Optional;
import org.gbif.vocabulary.lookup.LookupConcept;
import org.gbif.vocabulary.lookup.VocabularyLookup;
import org.gbif.vocabulary.model.Concept;
import org.gbif.vocabulary.model.LanguageRegion;

/**
 * Auxiliary test-only stub renamed to avoid shadowing the main test stub in
 * package org.gbif.vocabulary.lookup. This class is intentionally named
 * differently so tests import the correct stub.
 */
public class TestInMemoryVocabularyLookup implements VocabularyLookup {
  private final java.util.Map<String, LookupConcept> map = new java.util.HashMap<>();

  public TestInMemoryVocabularyLookup() {
  }

  public void add(String key, LookupConcept value) {
    map.put(key, value);
  }

  @Override
  public Optional<LookupConcept>  lookup(String key) {
    return Optional.ofNullable(map.get(key));
  }

  @Override
  public Optional<LookupConcept> lookup(String s, LanguageRegion languageRegion) {
    return Optional.empty();
  }

  @Override
  public void close() {

  }
}
