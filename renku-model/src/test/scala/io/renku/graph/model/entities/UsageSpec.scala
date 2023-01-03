/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectCreatedDates
import io.renku.graph.model.testentities.StepPlanCommandParameter.CommandInput
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities}
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UsageSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "decode" should {
    implicit val graph: GraphClass = GraphClass.Default

    "turn JsonLD Usage entity into the Usage object" in {
      forAll(entityLocations, entityChecksums) { (location, checksum) =>
        val activity = executionPlanners(stepPlanEntities(CommandInput.fromLocation(location)),
                                         projectCreatedDates().generateOne
        ).generateOne
          .planInputParameterValuesFromChecksum(location -> checksum)
          .buildProvenanceUnsafe()

        activity.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.Usage]] shouldBe activity.usages.map(_.to[entities.Usage]).asRight
      }
    }
  }
}
