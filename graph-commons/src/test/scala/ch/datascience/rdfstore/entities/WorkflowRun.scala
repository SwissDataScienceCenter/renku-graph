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

import java.time.Instant
import java.time.temporal.ChronoUnit._

import cats.implicits._
import ch.datascience.graph.model.events.{CommitId, CommittedDate}

trait WorkflowRun {
  self: Activity with ProcessRun =>

  def workflowRunFile: WorkflowFile
}

object WorkflowRun {

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._

  def apply(id:              CommitId,
            committedDate:   CommittedDate,
            committer:       Person,
            project:         Project,
            agent:           Agent,
            comment:         String,
            workflowFile:    WorkflowFile,
            informedBy:      Activity,
            association:     Association,
            startTime:       Instant = Instant.now(),
            endTime:         Instant = Instant.now().plus(10, SECONDS),
            maybeInfluenced: Option[Activity] = None,
            usages:          List[Usage]): Activity with ProcessRun with WorkflowRun =
    new Activity(id, committedDate, committer, project, agent, comment, Some(informedBy), maybeInfluenced)
    with ProcessRun with WorkflowRun {
      override val processRunAssociation: Association  = association
      override val processRunUsages:      List[Usage]  = usages
      override val workflowRunFile:       WorkflowFile = workflowFile
    }

  private[entities] implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                           fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[WorkflowRun] =
    new PartialEntityConverter[WorkflowRun] {
      override def convert[T <: WorkflowRun]: T => Either[Exception, PartialEntity] =
        _ =>
          PartialEntity(
            EntityTypes of (wfprov / "WorkflowRun")
          ).asRight
    }

}
