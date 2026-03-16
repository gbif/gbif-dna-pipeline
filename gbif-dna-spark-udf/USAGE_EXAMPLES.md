Usage: GbifVocabularyLookupGenericUdf and broadcasting InMemoryVocabularyLookup

This module provides `GbifVocabularyLookupUdf`, a Hive-compatible GenericUDF that performs lookups
using an `org.gbif.vocabulary.lookup.InMemoryVocabularyLookup` instance.

Design notes
- The UDF exposes a public `setLookup(InMemoryVocabularyLookup lookup)` method so you can inject the
  lookup instance created on the driver (for example from a broadcast variable's value).
- At runtime, register a single UDF instance and call `setLookup(broadcast.value())` on it before registration.
  When Spark serializes the UDF to workers it will include the lookup if the UDF is serializable; to avoid
  serializing large lookup data use `broadcast` and call `setLookup` with `broadcast.value()` on the driver
  before you register the UDF.

Java example (driver code)

```java
import org.apache.spark.sql.SparkSession;
import org.gbif.vocabulary.spark.udf.GbifVocabularyLookupUdf;
import org.gbif.vocabulary.lookup.InMemoryVocabularyLookup;

public class Example {

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().appName("example").getOrCreate();

    // Build your lookup on the driver (this may perform network IO)
    InMemoryVocabularyLookup lookup = InMemoryVocabularyLookup.newBuilder()
            .from("https://api.gbif.org/v1/vocabularies", "target_gene").build();

    // Broadcast the lookup to executors (optional)
    org.apache.spark.broadcast.Broadcast<InMemoryVocabularyLookup> b = spark.sparkContext()
            .broadcast(lookup,
                    scala.reflect.ClassTag$.MODULE$.apply(InMemoryVocabularyLookup.class));

    // Create a single UDF instance, inject the broadcast value, and register
    GbifVocabularyLookupUdf udf = new GbifVocabularyLookupUdf();
    udf.setLookup(b.value());
    spark.udf().register("gbifVocabularyLookup", udf);

    // Use in SQL
    spark.sql("SELECT gbifVocabularyLookup('irbp') as mapped FROM some_table").show();
  }
}
```
