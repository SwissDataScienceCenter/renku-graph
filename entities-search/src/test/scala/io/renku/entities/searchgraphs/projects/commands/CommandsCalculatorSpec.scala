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

package io.renku.entities.searchgraphs
package projects
package commands

import Encoders._
import Generators.projectSearchInfoObjects
import cats.syntax.all._
import io.renku.entities.searchgraphs.UpdateCommand
import io.renku.entities.searchgraphs.UpdateCommand.Insert
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model
import io.renku.triplesstore.client.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.{MatchResult, Matcher, should}
import org.scalatest.wordspec.AnyWordSpec

class CommandsCalculatorSpec extends AnyWordSpec with should.Matchers {

  "calculateCommands" should {

    "return no commands " +
      "when both the model and TS projects are the same" in {

        val modelInfo = projectSearchInfoObjects.generateOne
        val tsInfo    = modelInfo.some

        CommandsCalculator().calculateCommands(modelInfo, tsInfo) shouldBe Nil
      }

    "return project info delete query and inserts commands " +
      "when the model and TS projects are different" in {

        val modelInfo = projectSearchInfoObjects.generateOne
        val tsInfo = modelInfo
          .copy(visibility = Gen.oneOf(model.projects.Visibility.all - modelInfo.visibility).generateOne)
          .some

        val commands = CommandsCalculator().calculateCommands(modelInfo, tsInfo)

        commands.head shouldBe UpdateCommand.Query(ProjectInfoDeleteQuery(modelInfo.id))
        commands.tail should produce(
          modelInfo.asQuads(searchInfoEncoder).map(Insert).toList
        )
      }

    "return inserts commands " +
      "when there's no TS projects found" in {

        val modelInfo = projectSearchInfoObjects.generateOne

        CommandsCalculator().calculateCommands(modelInfo, maybeTSInfo = None) should produce(
          modelInfo.asQuads(searchInfoEncoder).map(Insert).toList
        )
      }
  }

  private def produce(expected: List[UpdateCommand]) = new Matcher[List[UpdateCommand]] {
    def apply(actual: List[UpdateCommand]): MatchResult = {
      val matching = actual.toSet == expected.toSet
      MatchResult(matching, s"\nExpected: $expected\nActual: $actual", s"\nExpected: $expected\nActual: $actual")
    }
  }
}
