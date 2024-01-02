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

import Generators._
import io.renku.entities.searchgraphs.Generators.updateCommands
import io.renku.generators.Generators.Implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class UpdateCommandsProducerSpec extends AnyWordSpec with should.Matchers with TryValues with MockFactory {

  "toUpdateCommands" should {

    "generate update commands from the model data" in new TestCase {

      val modelInfo = projectSearchInfoObjects.generateOne

      val expectedCommands = givenCommandsCalculation(modelInfo, returning = updateCommands.generateList())

      commandsProducer.toUpdateCommands(modelInfo).toSet shouldBe expectedCommands.toSet
    }
  }

  private trait TestCase {

    private val commandsCalculator = mock[CommandsCalculator]
    val commandsProducer           = new UpdateCommandsProducerImpl(commandsCalculator)

    def givenCommandsCalculation(modelInfo: ProjectSearchInfo, returning: List[UpdateCommand]) = {
      (commandsCalculator.calculateCommands _).expects(modelInfo).returning(returning)
      returning
    }
  }
}
