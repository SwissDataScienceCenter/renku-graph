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

import Generators._
import cats.syntax.all._
import io.renku.entities.searchgraphs.Generators.updateCommands
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class UpdateCommandsProducerSpec extends AnyWordSpec with should.Matchers with TryValues with MockFactory {

  "toUpdateCommands" should {

    "fetch Search Info for the Project from TS and " +
      "generate update commands for the model data and fetched data" in new TestCase {

        val modelInfo = projectSearchInfoObjects.generateOne
        val tsInfo    = projectSearchInfoObjects.generateOption.map(_.copy(id = modelInfo.id))

        givenSearchInfoFetcher(modelInfo.id, returning = tsInfo.pure[Try])

        val expectedCommands = givenCommandsCalculation(modelInfo, tsInfo, returning = updateCommands.generateList())

        commandsProducer.toUpdateCommands(modelInfo).map(_.toSet).success.value shouldBe expectedCommands.toSet
      }

    "fail if Info fetching fails" in new TestCase {

      val modelInfo = projectSearchInfoObjects.generateOne
      val exception = exceptions.generateOne
      givenSearchInfoFetcher(modelInfo.id, returning = exception.raiseError[Try, Nothing])

      commandsProducer.toUpdateCommands(modelInfo).failure.exception shouldBe exception
    }
  }

  private trait TestCase {

    private val searchInfoFetcher  = mock[SearchInfoFetcher[Try]]
    private val commandsCalculator = mock[CommandsCalculator]
    val commandsProducer           = new UpdateCommandsProducerImpl[Try](searchInfoFetcher, commandsCalculator)

    def givenSearchInfoFetcher(id: model.projects.ResourceId, returning: Try[Option[ProjectSearchInfo]]) =
      (searchInfoFetcher.fetchTSSearchInfo _)
        .expects(id)
        .returning(returning)

    def givenCommandsCalculation(modelInfo:   ProjectSearchInfo,
                                 maybeTSInfo: Option[ProjectSearchInfo],
                                 returning:   List[UpdateCommand]
    ) = {
      (commandsCalculator.calculateCommands _).expects(modelInfo, maybeTSInfo).returning(returning)
      returning
    }
  }
}
