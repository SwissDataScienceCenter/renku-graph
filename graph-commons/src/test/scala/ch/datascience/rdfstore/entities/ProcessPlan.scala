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

import cats.data.NonEmptyList
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.events.CommitId
import ch.datascience.rdfstore.FusekiBaseUrl

sealed abstract class ProcessPlan(val commitId: CommitId, val cwlFile: CwlFile, val project: Project)

object ProcessPlan {
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def toProperties(
      entity:              ProcessPlan
  )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): NonEmptyList[(Property, JsonLD)] =
    NonEmptyList.of(
      rdfs / "label"      -> s"${entity.cwlFile}@${entity.commitId}".asJsonLD,
      schema / "isPartOf" -> entity.project.asJsonLD,
      prov / "atLocation" -> entity.cwlFile.asJsonLD
    )
}

final case class StandardProcessPlan(override val commitId: CommitId,
                                     override val cwlFile:  CwlFile,
                                     override val project:  Project)
    extends ProcessPlan(commitId, cwlFile, project)

object StandardProcessPlan {

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl,
                       fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[StandardProcessPlan] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        EntityId of (fusekiBaseUrl / "blob" / entity.commitId / entity.cwlFile),
        EntityTypes of (prov / "Entity", prov / "Plan", wfdesc / "Process"),
        ProcessPlan.toProperties(entity)
      )
    }
}

final case class ProcessPlanWorkflow(override val commitId: CommitId,
                                     override val cwlFile:  CwlFile,
                                     override val project:  Project)
    extends ProcessPlan(commitId, cwlFile, project)

object ProcessPlanWorkflow {

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl,
                       fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[ProcessPlanWorkflow] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        EntityId of (fusekiBaseUrl / "blob" / entity.commitId / entity.cwlFile),
        EntityTypes of (prov / "Entity", prov / "Plan", wfdesc / "Process", wfdesc / "Workflow"),
        ProcessPlan.toProperties(entity)
      )
    }
}
