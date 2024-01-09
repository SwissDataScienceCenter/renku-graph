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

package io.renku.data

import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, EntityTypes, JsonLD, JsonLDEncoder}
import io.renku.util.jsonld.Schemas

object MessageJsonLDEncoder extends MessageJsonLDEncoder

trait MessageJsonLDEncoder {

  implicit def messageJsonLDEncoder[T <: Message]: JsonLDEncoder[T] = JsonLDEncoder.instance[T] {
    case Message.StringMessage(value, severity) =>
      JsonLD.entity(
        EntityId.blank,
        toEntityTypes(severity),
        Schemas.schema / "description" -> value.asJsonLD
      )
    case Message.JsonMessage(value, severity) =>
      JsonLD.entity(
        EntityId.blank,
        toEntityTypes(severity),
        Schemas.schema / "description" -> value.noSpaces.asJsonLD
      )
  }

  private def toEntityTypes: Message.Severity => EntityTypes = {
    case Message.Severity.Error => EntityTypes of Schemas.renku / "Error"
    case Message.Severity.Info  => EntityTypes of Schemas.renku / "Info"
  }
}
