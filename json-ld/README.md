# json-ld

This is a Scala library to work with Json-LD. It's build on top of [Circe](https://circe.github.io/circe) and follows the design choices of its API.

## Encoding to Json-LD

The library allows to encode any Scala object to Json-LD. Encoders for common types like `String`, `Int`, `Long`, `java.time.Instant` and `java.time.LocalDate` are provided by the library. There are also facilities for encoding objects, arrays of objects, options, lists and sets. All the mentioned tools exists in the `io.renku.jsonld.JsonLD` object.

Example:

```
import io.renku.jsonld._

JsonLD.fromInt(1)
JsonLD.fromOption(Some("abc"))
JsonLD.arr(JsonLD.fromString("a"), JsonLD.fromString("b"), JsonLD.fromString("c"))
JsonLD.entity(
  EntityId of "http://entity/23424",
  EntityTypes of (Schema.from("http://schema.org") / "Project"),
  Schema.from("http://schema.org") / "name" -> JsonLD.fromString("value")
)
```

Improved readability can be achieved by providing encoders for the types together with implicits from the `io.renku.jsonld.syntax` package.

Example:

```
import io.renku.jsonld._
import io.renku.jsonld.syntax._

val schema: Schema = Schema.from("http://schema.org")

final case class MyType(name: String)

implicit val myTypeEncoder: JsonLDEncoder[MyType] = JsonLDEncoder.instance { entity =>
    JsonLD.entity(
      EntityId of "http://entity/23424",
      EntityTypes of (schema / "Project"),
      schema / "name" -> entity.name.asJsonLD
    )
  }
  
 MyType(name = "some name").asJsonLD
```

## Encoding to Json

Every JsonLD object can be turn to Json using it's `toJson` method.
