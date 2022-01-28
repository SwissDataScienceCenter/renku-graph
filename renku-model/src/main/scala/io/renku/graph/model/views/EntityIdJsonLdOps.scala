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

package io.renku.graph.model.views

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.jsonld.{EntityId, EntityIdEncoder, JsonLDDecoder}
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

trait EntityIdJsonLdOps[TT <: StringTinyType] {
  self: TinyTypeFactory[TT] =>

  implicit lazy val encoder: EntityIdEncoder[TT] =
    EntityIdEncoder.instance[TT](id => EntityId.of(id.value))

  implicit lazy val decoder: JsonLDDecoder[TT] =
    JsonLDDecoder.instance {
      JsonLDDecoder.decodeEntityId >>> {
        _.flatMap(entityId =>
          self
            .from(entityId.toString)
            .leftMap(e => DecodingFailure(s"Cannot decode $entityId entityId: ${e.getMessage}", Nil))
        )
      }
    }
}
