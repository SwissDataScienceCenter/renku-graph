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

package ch.datascience.rdfstore.entities

import cats.syntax.all._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.rdfstore.FusekiBaseUrl
import io.renku.jsonld.{EntityId, EntityTypes}

trait Artifact

object Artifact {

  private[entities] implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                           fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[Artifact] =
    new PartialEntityConverter[Artifact] {
      override def convert[T <: Artifact]: T => Either[Exception, PartialEntity] =
        _ => PartialEntity(EntityTypes of wfprov / "Artifact").asRight

      override val toEntityId: Artifact => Option[EntityId] = _ => None
    }
}
