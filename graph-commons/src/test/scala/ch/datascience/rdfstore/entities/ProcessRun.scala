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
import ch.datascience.rdfstore.entities.Association.{ChildRunPlanAssociation, ProcessRunPlanAssociation, WorkflowRunPlanAssociation}
import ch.datascience.rdfstore.entities.RunPlan.{ProcessRunPlan, WorkflowRunPlan}
import ch.datascience.rdfstore.entities.WorkflowRun.ActivityWorkflowRun

import scala.language.postfixOps

sealed trait ProcessRun[RunPlanType <: Entity with RunPlan] {
  self: Activity =>
  type AssociationType <: Association[RunPlanType]
  def processRunAssociation: AssociationType
  def processRunUsages:      List[Usage]

}

object ProcessRun {

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  trait ChildProcessRun extends ProcessRun[Entity with ProcessRunPlan] {
    self: Activity =>
    override type AssociationType = ChildRunPlanAssociation
    def processRunStep:        Step
    def processRunWorkflowRun: ActivityWorkflowRun
  }

  trait WorkflowProcessRun extends ProcessRun[Entity with WorkflowRunPlan] {
    self: Activity =>
    override type AssociationType = WorkflowRunPlanAssociation
  }

  trait StandAloneProcessRun extends ProcessRun[Entity with ProcessRunPlan] {
    self: Activity =>
    override type AssociationType = ProcessRunPlanAssociation
  }

  def child(
      associationFactory: ActivityWorkflowRun => Step => ChildRunPlanAssociation,
      maybeInvalidation:  Option[Entity with Artifact] = None
  )(workflowRun:          ActivityWorkflowRun)(step: Step): Activity with ChildProcessRun =
    new Activity(workflowRun.commitId,
                 workflowRun.committedDate,
                 workflowRun.committer,
                 workflowRun.project,
                 workflowRun.agent,
                 workflowRun.comment,
                 workflowRun.maybeInformedBy,
                 workflowRun.maybeInfluenced,
                 maybeInvalidation,
                 maybeGenerationFactories = Nil) with ChildProcessRun {
      override val processRunAssociation: ChildRunPlanAssociation = associationFactory(workflowRun)(step)
      override val processRunUsages: List[Usage] = workflowRun.processRunAssociation.runPlan.runSubprocesses
        .get(step.value)
        .map(_.asUsages(step))
        .getOrElse(throw new IllegalStateException(s"Subprocess not found for step $step"))
      override val processRunStep:        Step                = step
      override val processRunWorkflowRun: ActivityWorkflowRun = workflowRun
    }

  def workflow(
      id:                CommitId,
      committedDate:     CommittedDate,
      committer:         Person,
      project:           Project,
      agent:             Agent,
      comment:           String,
      maybeInformedBy:   Option[Activity],
      association:       WorkflowRunPlanAssociation,
      maybeInfluenced:   Option[Activity] = None,
      maybeInvalidation: Option[Entity with Artifact] = None
  ): Activity with WorkflowProcessRun =
    new Activity(id,
                 committedDate,
                 committer,
                 project,
                 agent,
                 comment,
                 maybeInformedBy,
                 maybeInfluenced,
                 maybeInvalidation,
                 maybeGenerationFactories = Nil) with WorkflowProcessRun {
      override val processRunAssociation: WorkflowRunPlanAssociation = association
      override val processRunUsages:      List[Usage]                = association.runPlan.asUsages
    }

  def standAlone(
      id:                 CommitId,
      committedDate:      CommittedDate,
      committer:          Person,
      project:            Project,
      agent:              Agent,
      comment:            String,
      maybeInformedBy:    Option[Activity],
      associationFactory: Activity => ProcessRunPlanAssociation,
      maybeInfluenced:    Option[Activity] = None,
      maybeInvalidation:  Option[Entity with Artifact] = None
  ): Activity with StandAloneProcessRun =
    new Activity(id,
                 committedDate,
                 committer,
                 project,
                 agent,
                 comment,
                 maybeInformedBy,
                 maybeInfluenced,
                 maybeInvalidation,
                 maybeGenerationFactories = Nil) with StandAloneProcessRun {
      override val processRunAssociation: ProcessRunPlanAssociation = associationFactory(this)
      override val processRunUsages:      List[Usage]               = associationFactory(this).runPlan.asUsages
    }

  private[entities] implicit def childProcessRunConverter(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): PartialEntityConverter[Activity with ChildProcessRun] =
    new PartialEntityConverter[Activity with ChildProcessRun] {
      override def convert[T <: Activity with ChildProcessRun]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            EntityTypes of (wfprov / "ProcessRun"),
            rdfs / "label"                -> s"${entity.processRunAssociation.runPlan.location}@${entity.commitId}".asJsonLD,
            prov / "qualifiedAssociation" -> entity.processRunAssociation.asJsonLD,
            prov / "atLocation"           -> entity.processRunAssociation.runPlan.location.asJsonLD,
            prov / "qualifiedUsage"       -> entity.processRunUsages.asJsonLD,
            prov / "wasPartOfWorkflowRun" -> entity.processRunWorkflowRun.asEntityId.asJsonLD
          ).asRight

      override def toEntityId: Activity with ChildProcessRun => Option[EntityId] =
        entity => (EntityId of fusekiBaseUrl / "activities" / "commit" / entity.commitId / entity.processRunStep).some
    }

  private[entities] implicit def standAloneProcessRunConverter(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): PartialEntityConverter[Activity with StandAloneProcessRun] =
    new PartialEntityConverter[Activity with StandAloneProcessRun] {
      override def convert[T <: Activity with StandAloneProcessRun]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            EntityTypes of (wfprov / "ProcessRun"),
            rdfs / "label"                -> s"${entity.processRunAssociation.runPlan.location}@${entity.commitId}".asJsonLD,
            prov / "qualifiedAssociation" -> entity.processRunAssociation.asJsonLD,
            prov / "atLocation"           -> entity.processRunAssociation.runPlan.location.asJsonLD,
            prov / "qualifiedUsage"       -> entity.processRunUsages.asJsonLD
          ).asRight

      override def toEntityId: Activity with StandAloneProcessRun => Option[EntityId] =
        entity => (EntityId of fusekiBaseUrl / "activities" / "commit" / entity.commitId).some
    }

  private[entities] implicit def workflowProcessRunConverter(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): PartialEntityConverter[Activity with WorkflowProcessRun] =
    new PartialEntityConverter[Activity with WorkflowProcessRun] {
      override def convert[T <: Activity with WorkflowProcessRun]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            EntityTypes of (wfprov / "ProcessRun"),
            rdfs / "label"                -> s"${entity.processRunAssociation.runPlan.location}@${entity.commitId}".asJsonLD,
            prov / "qualifiedAssociation" -> entity.processRunAssociation.asJsonLD,
            prov / "atLocation"           -> entity.processRunAssociation.runPlan.location.asJsonLD,
            prov / "qualifiedUsage"       -> entity.processRunUsages.asJsonLD
          ).asRight

      override def toEntityId: Activity with WorkflowProcessRun => Option[EntityId] =
        entity => (EntityId of fusekiBaseUrl / "activities" / "commit" / entity.commitId).some
    }

  implicit def childProcessRunEncoder(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): JsonLDEncoder[Activity with ChildProcessRun] =
    JsonLDEncoder.instance { entity =>
      entity
        .asPartialJsonLD[Activity]
        .combine(entity.asPartialJsonLD[Activity with ChildProcessRun])
        .getOrFail
    }

  implicit def standAloneProcessRunEncoder(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): JsonLDEncoder[Activity with StandAloneProcessRun] =
    JsonLDEncoder.instance { entity =>
      entity
        .asPartialJsonLD[Activity]
        .combine(entity.asPartialJsonLD[Activity with StandAloneProcessRun])
        .getOrFail
    }

  implicit def workflowProcessRunEncoder(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): JsonLDEncoder[Activity with WorkflowProcessRun] =
    JsonLDEncoder.instance { entity =>
      entity
        .asPartialJsonLD[Activity]
        .combine(entity.asPartialJsonLD[Activity with WorkflowProcessRun])
        .getOrFail
    }

  implicit def childEntityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl,
                                    fusekiBaseUrl:         FusekiBaseUrl): EntityIdEncoder[Activity with ChildProcessRun] =
    EntityIdEncoder.instance { entity =>
      entity
        .getEntityId[Activity with ChildProcessRun]
        .orElse(entity.getEntityId[Activity])
        .getOrElse(throw new IllegalStateException(s"No EntityId found for $entity"))
    }

  implicit def standAloneEntityIdEncoder(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): EntityIdEncoder[Activity with StandAloneProcessRun] =
    EntityIdEncoder.instance { entity =>
      entity
        .getEntityId[Activity with StandAloneProcessRun]
        .orElse(entity.getEntityId[Activity])
        .getOrElse(throw new IllegalStateException(s"No EntityId found for $entity"))
    }

  implicit def workflowEntityIdEncoder(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): EntityIdEncoder[Activity with WorkflowProcessRun] =
    EntityIdEncoder.instance { entity =>
      entity
        .getEntityId[Activity with WorkflowProcessRun]
        .orElse(entity.getEntityId[Activity])
        .getOrElse(throw new IllegalStateException(s"No EntityId found for $entity"))
    }

}
