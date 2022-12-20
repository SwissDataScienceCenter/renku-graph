/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.images

import cats.syntax.all._
import io.circe.{Decoder, Encoder, Json}
import io.circe.syntax._
import io.renku.graph.model.views.TinyTypeJsonLDOps
import io.renku.tinytypes._

trait ImageUri extends Any with TinyType {
  type V = String
}

object ImageUri extends From[ImageUri] with TinyTypeJsonLDOps[ImageUri] {

  def apply(value: String): ImageUri = from(value).fold(throw _, identity)

  override def from(value: String): Either[IllegalArgumentException, ImageUri] =
    Relative.from(value) orElse Absolute.from(value)

  final class Relative private (val value: String) extends AnyVal with ImageUri with RelativePathTinyType {
    override type V = String
  }
  object Relative extends TinyTypeFactory[Relative](new Relative(_)) with constraints.RelativePath[Relative]

  final class Absolute private (val value: String) extends AnyVal with ImageUri with UrlTinyType {
    override type V = String
  }
  object Absolute extends TinyTypeFactory[Absolute](new Absolute(_)) with constraints.Url[Absolute]

  implicit lazy val encoder: Encoder[ImageUri] = Encoder.instance {
    case uri: Relative => Json.fromString(uri.value)
    case uri: Absolute => uri.asJson
  }

  implicit lazy val decoder: Decoder[ImageUri] = Decoder.decodeString.emap { value =>
    (Relative.from(value) orElse Absolute.from(value)).leftMap(_ => s"Cannot decode $value to $ImageUri")
  }
}
