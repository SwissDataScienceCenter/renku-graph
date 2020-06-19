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

import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.CommandParameter.{EntityCommandParameter, Input}

final class Usage(val commandInput: EntityCommandParameter with Input, val maybeStep: Option[Step])

object Usage {

  def apply(commandInput: EntityCommandParameter with Input): Usage =
    new Usage(commandInput, maybeStep = None)

  def factory(commandInput: EntityCommandParameter with Input): Step => Usage =
    step => new Usage(commandInput, maybeStep = Some(step))

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLDEncoder[Usage] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        EntityId of fusekiBaseUrl / "activities" / "commit" / entity.commandInput.entity.commitId / entity.maybeStep / "inputs" / entity.commandInput.toString,
        EntityTypes of (prov / "Usage"),
        prov / "entity"  -> entity.commandInput.entity.asJsonLD,
        prov / "hadRole" -> s"${entity.commandInput}_${entity.commandInput.position}".asJsonLD
      )
    }
}
