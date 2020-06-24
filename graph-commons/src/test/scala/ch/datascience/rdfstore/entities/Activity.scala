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

import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.ProcessRun.{ChildProcessRun, StandAloneProcessRun, WorkflowProcessRun}
import ch.datascience.rdfstore.entities.WorkflowRun.ActivityWorkflowRun

import scala.language.postfixOps

class Activity(val commitId:        CommitId,
               val committedDate:   CommittedDate,
               val committer:       Person,
               val project:         Project,
               val agent:           Agent,
               val comment:         String,
               val maybeInformedBy: Option[Activity],
               val maybeInfluenced: Option[Activity])

object Activity {

  def apply(id:              CommitId,
            committedDate:   CommittedDate,
            committer:       Person,
            project:         Project,
            agent:           Agent,
            comment:         String = "some comment",
            maybeInformedBy: Option[Activity] = None,
            maybeInfluenced: Option[Activity] = None): Activity =
    new Activity(id, committedDate, committer, project, agent, comment, maybeInformedBy, maybeInfluenced)

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  private[entities] implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                           fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[Activity] =
    new PartialEntityConverter[Activity] {
      override def convert[T <: Activity]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            EntityId of (fusekiBaseUrl / "activities" / "commit" / entity.commitId),
            EntityTypes of (prov / "Activity"),
            rdfs / "label"             -> entity.commitId.asJsonLD,
            rdfs / "comment"           -> entity.comment.asJsonLD,
            prov / "startedAtTime"     -> entity.committedDate.asJsonLD,
            prov / "endedAtTime"       -> entity.committedDate.asJsonLD,
            prov / "wasInformedBy"     -> entity.maybeInformedBy.asJsonLD,
            prov / "wasAssociatedWith" -> JsonLD.arr(entity.agent.asJsonLD, entity.committer.asJsonLD),
            prov / "influenced"        -> entity.maybeInfluenced.asJsonLD,
            schema / "isPartOf"        -> entity.project.asJsonLD
          ).asRight
    }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLDEncoder[Activity] =
    JsonLDEncoder.instance {
      case a: ActivityWorkflowRun                => a.asJsonLD
      case a: Activity with ChildProcessRun      => a.asJsonLD
      case a: Activity with WorkflowProcessRun   => a.asJsonLD
      case a: Activity with StandAloneProcessRun => a.asJsonLD
      case a: Activity                           => a.asPartialJsonLD[Activity] getOrFail
      case a => throw new Exception(s"Cannot serialize ${a.getClass} Activity")
    }
}
