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

import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.rdfstore.FusekiBaseUrl

abstract class Activity(val id:              CommitId,
                        val committedDate:   CommittedDate,
                        val committer:       Person,
                        val project:         Project,
                        val agent:           Agent,
                        val comment:         String,
                        val maybeInformedBy: Option[Activity])

object Activity {

  def apply(id:              CommitId,
            committedDate:   CommittedDate,
            committer:       Person,
            project:         Project,
            agent:           Agent,
            comment:         String = "some comment",
            maybeInformedBy: Option[Activity] = None): Activity =
    StandardActivity(id, committedDate, committer, project, agent, comment, maybeInformedBy)

  import cats.data.NonEmptyList
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def toProperties(
      entity:              Activity
  )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): NonEmptyList[(Property, JsonLD)] =
    NonEmptyList.of(
      prov / "startedAtTime" -> entity.committedDate.asJsonLD,
      prov / "endedAtTime"   -> entity.committedDate.asJsonLD,
      prov / "wasInformedBy" -> entity.maybeInformedBy.asJsonLD,
      prov / "agent"         -> JsonLD.arr(entity.agent.asJsonLD, entity.committer.asJsonLD),
      rdfs / "comment"       -> entity.comment.asJsonLD,
      schema / "isPartOf"    -> entity.project.asJsonLD
    )

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLDEncoder[Activity] =
    JsonLDEncoder.instance {
      case a: StandardActivity           => a.asJsonLD
      case a: ProcessRunActivity         => a.asJsonLD
      case a: ProcessRunWorkflowActivity => a.asJsonLD
      case a => throw new Exception(s"Cannot serialize ${a.getClass} Activity")
    }
}

final case class StandardActivity(override val id:              CommitId,
                                  override val committedDate:   CommittedDate,
                                  override val committer:       Person,
                                  override val project:         Project,
                                  override val agent:           Agent,
                                  override val comment:         String,
                                  override val maybeInformedBy: Option[Activity] = None)
    extends Activity(id, committedDate, committer, project, agent, comment, maybeInformedBy)

object StandardActivity {

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl,
                       fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[StandardActivity] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        EntityId of (fusekiBaseUrl / "activities" / "commit" / entity.id),
        EntityTypes of (prov / "Activity"),
        Activity.toProperties(entity) :+ rdfs / "label" -> entity.id.asJsonLD
      )
    }
}
