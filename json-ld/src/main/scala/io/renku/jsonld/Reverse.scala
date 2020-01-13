package io.renku.jsonld

import io.circe.{Encoder, Json}
import io.renku.jsonld.JsonLD.JsonLDEntity

final case class Reverse(properties: List[(Property, JsonLDEntity)]) extends Product with Serializable

object Reverse {

  def of(first: (Property, JsonLDEntity), other: (Property, JsonLDEntity)*): Reverse =
    Reverse((first +: other).toList)

  lazy val empty: Reverse = Reverse(Nil)

  implicit val jsonEncoder: Encoder[Reverse] = Encoder.instance {
    case Reverse(Nil)                  => Json.Null
    case Reverse((prop, value) +: Nil) => Json.obj(prop.url -> value.toJson)
    case Reverse(props)                => Json.arr(props.map { case (prop, value) => Json.obj(prop.url -> value.toJson) }: _*)
  }
}
