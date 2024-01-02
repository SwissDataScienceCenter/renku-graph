/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.views

import cats.syntax.all._
import io.renku.jsonld.{JsonLDDecoder, JsonLDEncoder}
import io.renku.tinytypes.{From, TinyType}

trait TinyTypeJsonLDOps[TT <: TinyType] extends JsonLDTinyTypeDecoder[TT] with JsonLDTinyTypeEncoder[TT] {
  self: From[TT] =>
}

trait JsonLDTinyTypeDecoder[TT <: TinyType] {
  self: From[TT] =>

  implicit def decoder(implicit valueDecoder: JsonLDDecoder[TT#V]): JsonLDDecoder[TT] =
    valueDecoder.emap(value => from(value).leftMap(_.getMessage))
}

trait JsonLDTinyTypeEncoder[TT <: TinyType] {
  self: From[TT] =>

  implicit def encoder(implicit valueEncoder: JsonLDEncoder[TT#V]): JsonLDEncoder[TT] =
    valueEncoder.contramap[TT](_.value)
}
