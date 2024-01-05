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

package io.renku.util.jsonld

import cats.syntax.all._
import io.renku.jsonld.{JsonLDDecoder, JsonLDEncoder}
import io.renku.tinytypes.{From, TinyType}

object TinyTypeJsonLDCodec {

  implicit def decoder[TT <: TinyType](implicit valueDecoder: JsonLDDecoder[TT#V], from: From[TT]): JsonLDDecoder[TT] =
    valueDecoder.emap(value => from.from(value).leftMap(_.getMessage))

  implicit def encoder[TT <: TinyType](implicit valueEncoder: JsonLDEncoder[TT#V]): JsonLDEncoder[TT] =
    valueEncoder.contramap[TT](_.value)
}
