/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.cli.model

import CliAssociation.AssociatedPlan
import Ontologies.{Prov, Schema}
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.associations._
import io.renku.jsonld._
import io.renku.jsonld.syntax._

final case class CliAssociation(
    id:    ResourceId,
    agent: CliAgent,
    plan:  AssociatedPlan
) extends CliModel

object CliAssociation {

  sealed trait AssociatedPlan {
    def fold[A](fa: CliStepPlan => A, fb: CliWorkflowFileStepPlan => A): A
  }
  object AssociatedPlan {
    final case class Step(plan: CliStepPlan) extends AssociatedPlan {
      def fold[A](fa: CliStepPlan => A, fb: CliWorkflowFileStepPlan => A): A = fa(plan)
    }

    final case class WorkflowFile(plan: CliWorkflowFileStepPlan) extends AssociatedPlan {
      def fold[A](fa: CliStepPlan => A, fb: CliWorkflowFileStepPlan => A): A = fb(plan)
    }

    def apply(plan: CliStepPlan):             AssociatedPlan = Step(plan)
    def apply(plan: CliWorkflowFileStepPlan): AssociatedPlan = WorkflowFile(plan)

    private val entityTypes: EntityTypes = EntityTypes.of(Prov.Plan, Schema.Action, Schema.CreativeWork)

    private def selectCandidates(ets: EntityTypes): Boolean =
      CliStepPlan.matchingEntityTypes(ets) ||
        CliWorkflowFileStepPlan.matchingEntityTypes(ets)

    implicit val jsonLDDecoder: JsonLDDecoder[AssociatedPlan] = {
      val da = CliStepPlan.jsonLDDecoder.emap(p => AssociatedPlan(p).asRight)
      val db = CliWorkflowFileStepPlan.jsonLDDecoder.emap(p => AssociatedPlan(p).asRight)

      JsonLDDecoder.cacheableEntity(entityTypes, _.getEntityTypes.map(selectCandidates)) { cursor =>
        val currentTypes = cursor.getEntityTypes
        (currentTypes.map(CliStepPlan.matchingEntityTypes) ->
          currentTypes.map(CliWorkflowFileStepPlan.matchingEntityTypes))
          .flatMapN {
            case (true, _) => da(cursor)
            case (_, true) => db(cursor)
            case _ => Left(DecodingFailure(s"Invalid entity types for decoding associated plan: $currentTypes", Nil))
          }
      }
    }

    implicit val jsonLDEncoder: JsonLDEncoder[AssociatedPlan] = JsonLDEncoder.instance(_.fold(_.asJsonLD, _.asJsonLD))
  }

  private val entityTypes: EntityTypes = EntityTypes.of(Prov.Association)

  implicit val jsonLDDecoder: JsonLDDecoder[CliAssociation] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId <- cursor.downEntityId.as[ResourceId]
        plan       <- cursor.downField(Prov.hadPlan).as[AssociatedPlan]
        agent      <- cursor.downField(Prov.agent).as[CliAgent]
      } yield CliAssociation(resourceId, agent, plan)
    }

  implicit val jsonLDEncoder: JsonLDEncoder[CliAssociation] =
    JsonLDEncoder.instance { assoc =>
      JsonLD.entity(
        assoc.id.asEntityId,
        entityTypes,
        Prov.hadPlan -> assoc.plan.asJsonLD,
        Prov.agent   -> assoc.agent.asJsonLD
      )
    }
}
