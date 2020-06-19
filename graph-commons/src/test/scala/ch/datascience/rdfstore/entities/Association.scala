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

final class Association private (val commitId:  CommitId,
                                 val agent:     Agent,
                                 val runPlan:   Entity with RunPlan,
                                 val maybeStep: Option[Step] = None)

object Association {

  def factory(agent: Agent, runPlan: CommitId => Entity with RunPlan): CommitId => Association =
    commitId => new Association(commitId, agent, runPlan(commitId), maybeStep = None)

  def factory(agent: Agent, runPlan: CommitId => Entity with RunPlan): (CommitId, Step) => Association = {
    case (commitId, step) => new Association(commitId, agent, runPlan(commitId), Some(step))
  }

  def factory(agent:   Agent,
              runPlan: (CommitId, WorkflowFile) => Entity with RunPlan): (CommitId, WorkflowFile) => Association =
    (commitId, workflowFile) => new Association(commitId, agent, runPlan(commitId, workflowFile), maybeStep = None)

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLDEncoder[Association] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        EntityId of fusekiBaseUrl / "activities" / "commit" / entity.commitId / entity.maybeStep / "association",
        EntityTypes of (prov / "Association"),
        prov / "agent"   -> entity.agent.asJsonLD,
        prov / "hadPlan" -> entity.runPlan.asJsonLD
      )
    }
}
