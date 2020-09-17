/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
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

package io.renku.jsonld

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.jsonld.JsonLD.JsonLDValue
import io.renku.jsonld.JsonLDDecoder.Result

/**
  * A type class that provides a conversion from a [[Cursor]] to an object of type `A`
  */
trait JsonLDDecoder[A] extends Serializable {
  def apply(cursor: Cursor): Result[A]
}

object JsonLDDecoder {

  type Result[A] = Either[DecodingFailure, A]

  implicit val decodeJsonLD: JsonLDDecoder[JsonLD] = (cursor: Cursor) => cursor.jsonLD.asRight[DecodingFailure]
  implicit val decodeString: JsonLDDecoder[String] = (cursor: Cursor) =>
    cursor.jsonLD match {
      case JsonLDValue(value: String, _) => Right(value)
      case json => Left(DecodingFailure(s"Cannot decode $json to String", Nil))
    }
}
