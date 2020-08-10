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

final class Usage private (val activity:     Activity,
                           val commandInput: EntityCommandParameter with Input,
                           val maybeStep:    Option[Step])

object Usage {

  def apply(activity: Activity, commandInput: EntityCommandParameter with Input): Usage =
    new Usage(activity, commandInput, maybeStep = None)

  def factory(activity: Activity, commandInput: EntityCommandParameter with Input): Step => Usage =
    step => new Usage(activity, commandInput, maybeStep = Some(step))

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLDEncoder[Usage] =
    JsonLDEncoder.instance { entity =>
      val entityId = entity.maybeStep match {
        case None =>
          EntityId of fusekiBaseUrl / "activities" / entity.activity.commitId / "inputs" / entity.commandInput.toString
        case Some(step) =>
          EntityId of fusekiBaseUrl / "activities" / entity.activity.commitId / "steps" / step / "inputs" / entity.commandInput.toString
      }
      JsonLD.entity(
        entityId,
        EntityTypes of (prov / "Usage"),
        prov / "entity"  -> entity.commandInput.entity.asJsonLD,
        prov / "hadRole" -> entity.commandInput.toString.asJsonLD
      )
    }
}
