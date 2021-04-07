/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.Activity.Id
import ch.datascience.rdfstore.entities.ProcessRun.{ChildProcessRun, WorkflowProcessRun}

import java.time.Instant
import java.time.temporal.ChronoUnit._

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
      commitId:                 Id,
      committedDate:            Activity.StartTime,
      committer:                Person,
      project:                  Project,
      agent:                    Agent,
      comment:                  String,
      workflowFile:             WorkflowFile,
      informedBy:               Activity,
      associationFactory:       Project => Activity => WorkflowFile => WorkflowRunPlanAssociation,
      startTime:                Instant = Instant.now(),
      endTime:                  Instant = Instant.now().plus(10, SECONDS),
      maybeInfluenced:          Option[Activity] = None,
      processRunsFactories:     List[ActivityWorkflowRun => Step => Activity with ChildProcessRun],
      maybeInvalidation:        Option[Entity with Artifact] = None,
      maybeGenerationFactories: List[Activity => Generation] = Nil
  ): ActivityWorkflowRun =
    new Activity(commitId,
                 committedDate,
                 committer,
                 project,
                 agent,
                 comment,
                 Some(informedBy),
                 maybeInfluenced,
                 maybeInvalidation,
                 maybeGenerationFactories
    ) with WorkflowProcessRun with WorkflowRun {
      override val processRunAssociation: WorkflowRunPlanAssociation = associationFactory(project)(this)(workflowFile)
      override val processRunUsages:      List[Usage]                = processRunAssociation.runPlan.asUsages(this)
      val workflowRunFile:                WorkflowFile               = workflowFile
      val processRuns: List[Activity with ChildProcessRun] = processRunsFactories.zipWithIndex.map {
        case (factory, idx) => factory(this)(Step(idx))
      }
    }

  private[entities] implicit def converter(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      gitLabApiUrl:  GitLabApiUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): PartialEntityConverter[Activity with WorkflowRun] =
    new PartialEntityConverter[Activity with WorkflowRun] {
      override def convert[T <: Activity with WorkflowRun]: T => Either[Exception, PartialEntity] =
        entity =>
          for {
            reverse <- Reverse.of((wfprov / "wasPartOfWorkflowRun") -> entity.processRuns.map(_.asJsonLD))
          } yield PartialEntity(
            EntityTypes of (wfprov / "WorkflowRun"),
            reverse,
            rdfs / "label" -> s"${entity.workflowRunFile}@${entity.id}".asJsonLD
          )

      override lazy val toEntityId: Activity with WorkflowRun => Option[EntityId] = _ => None
    }

  implicit def encoder(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      gitLabApiUrl:  GitLabApiUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): JsonLDEncoder[ActivityWorkflowRun] =
    JsonLDEncoder.instance { entity =>
      entity
        .asPartialJsonLD[Activity]
        .combine(entity.asPartialJsonLD[Activity with WorkflowProcessRun])
        .combine(entity.asPartialJsonLD[Activity with WorkflowRun])
        .getOrFail
    }

  implicit def entityIdEncoder(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      gitLabApiUrl:  GitLabApiUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): EntityIdEncoder[ActivityWorkflowRun] =
    EntityIdEncoder.instance { entity =>
      entity
        .getEntityId[Activity with WorkflowProcessRun]
        .orElse(entity.getEntityId[Activity with WorkflowRun])
        .orElse(entity.getEntityId[Activity])
        .getOrElse(throw new IllegalStateException(s"No EntityId found for $entity"))
    }
}
