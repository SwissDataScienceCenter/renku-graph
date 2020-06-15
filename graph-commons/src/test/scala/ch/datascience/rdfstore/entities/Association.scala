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

import ch.datascience.graph.model.events.CommitId

final class Association private (val commitId:    CommitId,
                                 val agent:       Agent,
                                 val processPlan: ProcessPlan,
                                 val maybeStep:   Option[String] = None)

object Association {

  def apply(commitId: CommitId, agent: Agent, processPlan: ProcessPlan, maybeStep: Option[String] = None): Association =
    new Association(commitId, agent, processPlan, maybeStep)

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLDEncoder[Association] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        EntityId of (fusekiBaseUrl / "commit" / entity.commitId / entity.maybeStep
          .map(step => s"$step/association")
          .getOrElse("association")),
        EntityTypes of (prov / "Association"),
        prov / "agent" -> entity.agent.asJsonLD,
        prov / "hadPlan" -> (entity.processPlan match {
          case plan: StandardProcessPlan => plan.asJsonLD
          case plan: ProcessPlanWorkflow => plan.asJsonLD
          case plan: RunPlan             => plan.asJsonLD
        })
      )
    }
}
