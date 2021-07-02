/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.model.views

import cats.syntax.all._
import ch.datascience.tinytypes._
import io.renku.jsonld.JsonLDDecoder
import io.renku.jsonld.JsonLDDecoder._

object TinyTypeJsonLDDecoders extends TinyTypeJsonLDDecoders

trait TinyTypeJsonLDDecoders {

  implicit def stringDecoder[TT <: StringTinyType](implicit tinyTypeFactory: From[TT]): JsonLDDecoder[TT] =
    decodeString.emap(value => tinyTypeFactory.from(value).leftMap(_.getMessage))

  implicit def localDateDecoder[TT <: LocalDateTinyType](implicit tinyTypeFactory: From[TT]): JsonLDDecoder[TT] =
    decodeLocalDate.emap(value => tinyTypeFactory.from(value).leftMap(_.getMessage))

  implicit def instantDecoder[TT <: InstantTinyType](implicit tinyTypeFactory: From[TT]): JsonLDDecoder[TT] =
    decodeInstant.emap(value => tinyTypeFactory.from(value).leftMap(_.getMessage))

  implicit def intDecoder[TT <: IntTinyType](implicit tinyTypeFactory: From[TT]): JsonLDDecoder[TT] =
    decodeInt.emap(value => tinyTypeFactory.from(value).leftMap(_.getMessage))

  implicit def longDecoder[TT <: LongTinyType](implicit tinyTypeFactory: From[TT]): JsonLDDecoder[TT] =
    decodeLong.emap(value => tinyTypeFactory.from(value).leftMap(_.getMessage))

  implicit def booleanDecoder[TT <: BooleanTinyType](implicit tinyTypeFactory: From[TT]): JsonLDDecoder[TT] =
    decodeBoolean.emap(value => tinyTypeFactory.from(value).leftMap(_.getMessage))
}
