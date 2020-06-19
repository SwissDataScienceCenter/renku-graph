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
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.Association.WorkflowRunPlanAssociation
import ch.datascience.rdfstore.entities.ProcessRun.{ChildProcessRun, WorkflowProcessRun}
import ch.datascience.rdfstore.entities.RunPlan.WorkflowRunPlan
import io.renku.jsonld
import io.renku.jsonld.Reverse

trait WorkflowRun {
  self: Activity with WorkflowProcessRun =>

  def workflowRunFile: WorkflowFile
  def processRuns:     List[Activity with ChildProcessRun]
}

object WorkflowRun {
  type ActivityWorkflowRun = Activity with WorkflowProcessRun with WorkflowRun

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def apply(
      commitId:             CommitId,
      committedDate:        CommittedDate,
      committer:            Person,
      project:              Project,
      agent:                Agent,
      comment:              String,
      workflowFile:         WorkflowFile,
      informedBy:           Activity,
      associationFactory:   Project => Activity => WorkflowFile => WorkflowRunPlanAssociation,
      startTime:            Instant = Instant.now(),
      endTime:              Instant = Instant.now().plus(10, SECONDS),
      maybeInfluenced:      Option[Activity] = None,
      processRunsFactories: List[ActivityWorkflowRun => Step => Activity with ChildProcessRun]
  ): ActivityWorkflowRun =
    new Activity(commitId, committedDate, committer, project, agent, comment, Some(informedBy), maybeInfluenced)
    with WorkflowProcessRun with WorkflowRun {
      override val processRunAssociation: WorkflowRunPlanAssociation = associationFactory(project)(this)(workflowFile)
      override val processRunUsages:      List[Usage]                = processRunAssociation.runPlan.asUsages
      val workflowRunFile:                WorkflowFile               = workflowFile
      val processRuns: List[Activity with ChildProcessRun] = processRunsFactories.zipWithIndex {
        case (factory, idx) => factory(this)(Step(idx))
      }
    }

  private[entities] implicit val converter: PartialEntityConverter[Activity with WorkflowRun] =
    new PartialEntityConverter[Activity with WorkflowRun] {
      override def convert[T <: Activity with WorkflowRun]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            EntityTypes of (wfprov / "WorkflowRun"),
            rdfs / "label" -> s"${entity.workflowRunFile}@${entity.commitId}".asJsonLD,
            Reverse.of(prov / "wasPartOfWorkflowRun" -> entity.processRuns)
          ).asRight
    }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl,
                       fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[ActivityWorkflowRun] =
    JsonLDEncoder.instance { entity =>
      entity
        .asPartialJsonLD[Activity]
        .combine(entity.asPartialJsonLD[Activity with ProcessRun[Entity with WorkflowRunPlan]])
        .combine(entity.asPartialJsonLD[Activity with WorkflowRun])
        .getOrFail

    }
}
