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
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.testentities._
import ch.datascience.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import Generators._
import io.renku.jsonld.syntax._

class EntitySpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {
  "Entity.decode" should {
    "turn JsonLD InputEntity entity into the Entity object " in {
      forAll(inputEntities) { entity =>
        entity.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.Entity]] shouldBe List(entity.to[entities.Entity.InputEntity]).asRight
      }
    }

    "turn JsonLD Output entity into the Entity object " in {
      forAll(locationCommandOutputObjects) { commandOutput =>
        val activity = executionPlanners(runPlanEntities(commandOutput)).generateOne.buildProvenanceUnsafe()

        activity.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.Entity]] shouldBe activity.generations
          .map(_.entity.to[entities.Entity.OutputEntity])
          .asRight
      }
    }
  }
}
