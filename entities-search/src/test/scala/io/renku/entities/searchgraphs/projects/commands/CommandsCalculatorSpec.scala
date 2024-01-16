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

package io.renku.entities.searchgraphs
package projects
package commands

import Encoders._
import Generators.projectSearchInfoObjects
import io.renku.entities.searchgraphs.UpdateCommand
import io.renku.entities.searchgraphs.UpdateCommand.Insert
import io.renku.generators.Generators.Implicits._
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.{MatchResult, Matcher, should}
import org.scalatest.wordspec.AnyWordSpec

class CommandsCalculatorSpec extends AnyWordSpec with should.Matchers {

  "calculateCommands" should {

    "return project info delete query and inserts commands" in {

      val modelInfo = projectSearchInfoObjects.generateOne

      val commands = CommandsCalculator().calculateCommands(modelInfo)

      commands.head shouldBe UpdateCommand.Query(ProjectInfoDeleteQuery(modelInfo.id))
      commands.tail should produce(
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
