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

import ch.datascience.graph.config.GitLabApiUrl
import ch.datascience.graph.model.events.CommitId
import ch.datascience.rdfstore.entities.RunPlan.{ProcessRunPlan, WorkflowRunPlan}
import ch.datascience.rdfstore.entities.WorkflowRun.ActivityWorkflowRun

sealed trait Association[RunPlanType <: Entity with RunPlan] {
  val commitId:         CommitId
  val associationAgent: Agent
  val runPlan:          RunPlanType
}

object Association {

  trait ChildRunPlanAssociation extends Association[Entity with ProcessRunPlan] {
    val workflowStep: Step
  }

  trait WorkflowRunPlanAssociation extends Association[Entity with WorkflowRunPlan]
  trait ProcessRunPlanAssociation  extends Association[Entity with ProcessRunPlan]

  def child(
      agent:  Agent
  )(workflow: ActivityWorkflowRun)(step: Step): ChildRunPlanAssociation =
    new ChildRunPlanAssociation {
      override val commitId:         CommitId = workflow.commitId
      override val associationAgent: Agent    = agent
      override val workflowStep:     Step     = step
      override val runPlan: Entity with ProcessRunPlan =
        workflow.processRunAssociation.runPlan.runSubprocesses(step.value)
    }

  def workflow(
      agent:          Agent,
      runPlanFactory: Project => Activity => WorkflowFile => Entity with WorkflowRunPlan
  ): Project => Activity => WorkflowFile => WorkflowRunPlanAssociation =
    project =>
      activity =>
        workflowFile =>
          new WorkflowRunPlanAssociation {
            override val commitId:         CommitId                    = activity.commitId
            override val associationAgent: Agent                       = agent
            override val runPlan:          Entity with WorkflowRunPlan = runPlanFactory(project)(activity)(workflowFile)
          }

  def process[RunPlanType <: Entity with RunPlan](
      agent:          Agent,
      runPlanFactory: Activity => Entity with ProcessRunPlan
  ): Activity => ProcessRunPlanAssociation =
    activity =>
      new ProcessRunPlanAssociation {
        override val commitId:         CommitId                   = activity.commitId
        override val associationAgent: Agent                      = agent
        override val runPlan:          Entity with ProcessRunPlan = runPlanFactory(activity)
      }

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def childRunPlanAssociationEncoder(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      gitLabApiUrl:  GitLabApiUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): JsonLDEncoder[ChildRunPlanAssociation] = JsonLDEncoder.instance { entity =>
    JsonLD.entity(
      EntityId of fusekiBaseUrl / "activities" / "commit" / entity.commitId / entity.workflowStep / "association",
      EntityTypes of prov / "Association",
      prov / "agent"   -> entity.associationAgent.asJsonLD,
      prov / "hadPlan" -> entity.runPlan.asJsonLD
    )
  }

  implicit def workflowRunPlanAssociationEncoder(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      gitLabApiUrl:  GitLabApiUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): JsonLDEncoder[WorkflowRunPlanAssociation] = JsonLDEncoder.instance { entity =>
    JsonLD.entity(
      EntityId of fusekiBaseUrl / "activities" / "commit" / entity.commitId / "association",
      EntityTypes of prov / "Association",
      prov / "agent"   -> entity.associationAgent.asJsonLD,
      prov / "hadPlan" -> entity.runPlan.asJsonLD
    )
  }

  implicit def processRunPlanAssociationEncoder(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      gitLabApiUrl:  GitLabApiUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): JsonLDEncoder[ProcessRunPlanAssociation] = JsonLDEncoder.instance { entity =>
    JsonLD.entity(
      EntityId of fusekiBaseUrl / "activities" / "commit" / entity.commitId / "association",
      EntityTypes of prov / "Association",
      prov / "agent"   -> entity.associationAgent.asJsonLD,
      prov / "hadPlan" -> entity.runPlan.asJsonLD
    )
  }
}
