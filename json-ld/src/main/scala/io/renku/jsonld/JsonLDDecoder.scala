package io.renku.jsonld

import io.circe.DecodingFailure
import io.renku.jsonld.JsonLDDecoder.Result

/**
  * A type class that provides a conversion from a [[Cursor]] to an object of type `A`
  */
trait JsonLDDecoder[A] extends Serializable {

  def apply(cursor: Cursor): Result[A]
}

object JsonLDDecoder {

  import cats.implicits._

  type Result[A] = Either[DecodingFailure, A]

  implicit val jsonLDDecoder: JsonLDDecoder[JsonLD] = (cursor: Cursor) => cursor.jsonLD.asRight[DecodingFailure]
}
