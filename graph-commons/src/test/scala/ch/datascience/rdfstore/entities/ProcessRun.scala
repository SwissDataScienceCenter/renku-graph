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

trait ProcessRun {
  self: Activity =>

  def processRunAssociation: Association
  def processRunUsages:      List[Usage]
  def processRunMaybeStepId:               Option[String]      = None
  def processRunMaybeWasPartOfWorkflowRun: Option[WorkflowRun] = None
}

object ProcessRun {

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def apply(id:                        CommitId,
            committedDate:             CommittedDate,
            committer:                 Person,
            comment:                   String,
            maybeInformedBy:           Option[Activity],
            association:               Association,
            usages:                    List[Usage],
            maybeInfluenced:           Option[Activity] = None,
            maybeStepId:               Option[String] = None,
            maybeWasPartOfWorkflowRun: Option[Activity with ProcessRun with WorkflowRun] = None) =
    new Activity(id,
                 committedDate,
                 committer,
                 association.processPlan.project,
                 association.agent,
                 comment,
                 maybeInformedBy,
                 maybeInfluenced) with ProcessRun {
      override val processRunAssociation: Association = association
      override val processRunUsages:      List[Usage] = usages
    }

  private[entities] implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                           fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[ProcessRun] =
    new PartialEntityConverter[ProcessRun] {
      override def convert[T <: ProcessRun]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            EntityTypes of (wfprov / "ProcessRun"),
            prov / "qualifiedAssociation" -> entity.processRunAssociation.asJsonLD,
            prov / "atLocation"           -> entity.processRunAssociation.processPlan.workflowFile.asJsonLD,
            prov / "qualifiedUsage"       -> entity.processRunUsages.asJsonLD,
            prov / "wasPartOfWorkflowRun" -> entity.processRunMaybeWasPartOfWorkflowRun.asJsonLD
          ).asRight
    }
}
