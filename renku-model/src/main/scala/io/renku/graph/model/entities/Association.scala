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

package io.renku.graph.model.entities

import cats.data.ValidatedNel
import cats.syntax.all._
import io.renku.cli.model.CliAssociation
import io.renku.graph.model.Schemas.prov
import io.renku.graph.model.associations.ResourceId
import io.renku.graph.model._
import io.renku.http.client.GitLabApiUrl
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}
import io.renku.jsonld.ontology._
import io.renku.jsonld.syntax._

sealed trait Association {
  type AgentType
  val resourceId: ResourceId
  val agent:      AgentType
  val planId:     plans.ResourceId

  def fold[A](fa: Association.WithPersonAgent => A)(fb: Association.WithRenkuAgent => A): A
}

object Association {

  implicit def functions(implicit glUrl: GitLabApiUrl): EntityFunctions[Association] =
    new EntityFunctions[Association] {

      override val findAllPersons: Association => Set[Person] =
        AssociationLens.associationAgent.get(_).toOption.toSet

      override val encoder: GraphClass => JsonLDEncoder[Association] = Association.encoder(glUrl, _)
    }

  final case class WithRenkuAgent(resourceId: ResourceId, agent: Agent, planId: plans.ResourceId) extends Association {
    type AgentType = Agent

    def fold[A](fa: Association.WithPersonAgent => A)(fb: Association.WithRenkuAgent => A): A = fb(this)
  }
  final case class WithPersonAgent(resourceId: ResourceId, agent: Person, planId: plans.ResourceId)
      extends Association {
    type AgentType = Person

    def fold[A](fa: Association.WithPersonAgent => A)(fb: Association.WithRenkuAgent => A): A = fa(this)
  }

  val entityTypes: EntityTypes = EntityTypes of (prov / "Association")

  implicit def encoder(implicit glApiUrl: GitLabApiUrl, gc: GraphClass): JsonLDEncoder[Association] =
    JsonLDEncoder.instance {
      case WithRenkuAgent(resourceId, agent, planId) =>
        JsonLD.entity(
          resourceId.asEntityId,
          entityTypes,
          prov / "agent"   -> agent.asJsonLD,
          prov / "hadPlan" -> planId.asEntityId.asJsonLD
        )
      case WithPersonAgent(resourceId, agent, planId) =>
        JsonLD.entity(
          resourceId.asEntityId,
          entityTypes,
          prov / "agent"   -> agent.asJsonLD,
          prov / "hadPlan" -> planId.asEntityId.asJsonLD
        )
    }

  def fromCli(cliAssociation: CliAssociation)(implicit renkuUrl: RenkuUrl): ValidatedNel[String, Association] = {
    val planId = cliAssociation.plan.fold(_.id, _.id)
    cliAssociation.agent.fold(
      person => Person.fromCli(person).map(agent => WithPersonAgent(cliAssociation.id, agent, planId)),
      software => Agent.fromCli(software).map(agent => WithRenkuAgent(cliAssociation.id, agent, planId))
    )
  }

  def fromCliCheckExistingPlan(cliAssociation: CliAssociation, dependencyLinks: DependencyLinks)(implicit
      renkuUrl: RenkuUrl
  ): ValidatedNel[String, Association] =
    fromCli(cliAssociation).andThen { assoc =>
      dependencyLinks
        .findStepPlan(assoc.planId)
        .toValidNel(show"Association ${assoc.resourceId} points to a non-existing Plan ${assoc.planId}")
        .map(_ => assoc)
    }

  lazy val ontology: Type = Type.Def(
    Class(prov / "Association"),
    ObjectProperty(prov / "agent", Agent.ontology),
    ObjectProperty(prov / "hadPlan", StepPlan.ontology)
  )
}
