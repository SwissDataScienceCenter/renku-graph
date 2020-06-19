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
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.rdfstore.entities.WorkflowRun.ActivityWorkflowRun

import scala.language.postfixOps

trait ProcessRun {
  self: Activity =>

  def processRunAssociation:      Association
  def processRunUsages:           List[Usage]
  def processRunMaybeStep:        Option[Step]
  def processRunMaybeWorkflowRun: Option[ActivityWorkflowRun]
}

object ProcessRun {

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def apply(
      workflowRun:        ActivityWorkflowRun,
      commitId:           CommitId,
      step:               Step,
      committedDate:      CommittedDate,
      committer:          Person,
      agent:              Agent,
      comment:            String,
      maybeInformedBy:    Option[Activity],
      associationFactory: (CommitId, Step) => Association,
      usagesFactories:    List[Step => Usage],
      maybeInfluenced:    Option[Activity] = None
  ): Activity with ProcessRun = {
    val association = associationFactory(commitId, step)
    new Activity(commitId,
                 committedDate,
                 committer,
                 association.runPlan.project,
                 agent,
                 comment,
                 maybeInformedBy,
                 maybeInfluenced) with ProcessRun {
      override val processRunAssociation:      Association                 = association
      override val processRunUsages:           List[Usage]                 = usagesFactories.map(_.apply(step))
      override val processRunMaybeStep:        Option[Step]                = Some(step)
      override val processRunMaybeWorkflowRun: Option[ActivityWorkflowRun] = Some(workflowRun)
    }
  }

  def apply(
      id:                   CommitId,
      committedDate:        CommittedDate,
      committer:            Person,
      agent:                Agent,
      comment:              String,
      maybeInformedBy:      Option[Activity],
      association:          Association,
      usages:               List[Usage],
      maybeInfluenced:      Option[Activity] = None,
      maybeStep:            Option[Step] = None,
      maybeWorkflowRunPart: Option[ActivityWorkflowRun] = None
  ): Activity with ProcessRun =
    new Activity(id,
                 committedDate,
                 committer,
                 association.runPlan.project,
                 agent,
                 comment,
                 maybeInformedBy,
                 maybeInfluenced) with ProcessRun {
      override val processRunAssociation: Association  = association
      override val processRunUsages:      List[Usage]  = usages
      override val processRunMaybeStep:   Option[Step] = maybeStep
      override val processRunMaybeWorkflowRun: Option[ActivityWorkflowRun] =
        maybeWorkflowRunPart
    }

  private[entities] implicit def converter(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): PartialEntityConverter[Activity with ProcessRun] =
    new PartialEntityConverter[Activity with ProcessRun] {
      override def convert[T <: Activity with ProcessRun]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            EntityId of fusekiBaseUrl / "activities" / "commit" / entity.commitId / entity.processRunMaybeStep,
            EntityTypes of (wfprov / "ProcessRun"),
            rdfs / "label"                -> s"${entity.processRunAssociation.runPlan.location}@${entity.commitId}".asJsonLD,
            prov / "qualifiedAssociation" -> entity.processRunAssociation.asJsonLD,
            prov / "atLocation"           -> entity.processRunAssociation.runPlan.location.asJsonLD,
            prov / "qualifiedUsage"       -> entity.processRunUsages.asJsonLD,
            prov / "wasPartOfWorkflowRun" -> entity.processRunMaybeWorkflowRun.asJsonLD(
              JsonLDEncoder.encodeOption(WorkflowRun.encoder)
            )
          ).asRight
    }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl,
                       fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[Activity with ProcessRun] =
    JsonLDEncoder.instance { entity =>
      entity.asPartialJsonLD[Activity] combine entity.asPartialJsonLD[Activity with ProcessRun] getOrFail
    }
}
