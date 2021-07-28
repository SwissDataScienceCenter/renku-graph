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

package ch.datascience.graph.model.entities

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.Schemas.prov
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.testentities._
import io.circe.DecodingFailure
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLD, JsonLDEncoder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class AssociationSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "Association.decode" should {

    "turn JsonLD Association entity into the Association object" in {
      forAll(executionPlanners(planEntities(), projectEntities(visibilityAny)(anyForksCount))) { executionPlanner =>
        val activity = executionPlanner.buildProvenanceUnsafe()

        activity.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.Association]] shouldBe List(activity.association.to[entities.Association]).asRight
      }
    }

    "fail if there are no plan entity the Association points to" in {
      val association = executionPlanners(planEntities(), projectEntities(visibilityAny)(anyForksCount)).generateOne
        .buildProvenanceUnsafe()
        .association
        .to[entities.Association]

      val encoder = JsonLDEncoder.instance[entities.Association] { entity =>
        JsonLD.entity(
          entity.resourceId.asEntityId,
          entities.Association.entityTypes,
          prov / "agent"   -> entity.agent.asJsonLD,
          prov / "hadPlan" -> entity.plan.resourceId.asEntityId.asJsonLD
        )
      }

      val Left(error) = association
        .asJsonLD(encoder)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.Association]]
      error         shouldBe a[DecodingFailure]
      error.message shouldBe s"Association ${association.resourceId} without or with multiple Plans"
    }

    "fail if there are no Agent entity the Association points to" in {
      val association = executionPlanners(planEntities(), projectEntities(visibilityAny)(anyForksCount)).generateOne
        .buildProvenanceUnsafe()
        .association
        .to[entities.Association]

      val encoder = JsonLDEncoder.instance[entities.Association] { entity =>
        JsonLD.entity(
          entity.resourceId.asEntityId,
          entities.Association.entityTypes,
          prov / "agent"   -> entity.agent.resourceId.asEntityId.asJsonLD,
          prov / "hadPlan" -> entity.plan.asJsonLD
        )
      }

      val Left(error) = association
        .asJsonLD(encoder)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.Association]]
      error         shouldBe a[DecodingFailure]
      error.message shouldBe s"Association ${association.resourceId} without or with multiple Agents"
    }
  }
}
