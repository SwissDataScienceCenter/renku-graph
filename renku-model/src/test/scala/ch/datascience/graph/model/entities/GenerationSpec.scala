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

class GenerationSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {
  "Generation.decode" should {
    "turn JsonLD Generation entity into the Generation object " in {
      forAll(locationCommandOutputObjects) { commandOutput =>
        val activity = executionPlanners(planEntities(commandOutput)).generateOne.buildProvenanceUnsafe()

        activity.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.Generation]] shouldBe activity.generations
          .map(_.to[entities.Generation])
          .asRight
      }
    }
  }
}
