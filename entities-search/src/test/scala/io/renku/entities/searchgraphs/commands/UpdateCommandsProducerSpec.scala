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
package commands

import Generators.searchInfoObjectsGen
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Random, Try}

class UpdateCommandsProducerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  private val calculateCommands: CalculatorInfoSet => Try[List[UpdateCommand]] =
    CommandsCalculator.calculateCommands[Try]

  "toUpdateCommands" should {

    "fetch Datasets currently associated with the given Project in the TS, " +
      "zip them with the datasets found on the Project and " +
      "generate update commands for them - " +
      "case when all model infos have counterparts in the TS" in new TestCase {

        val modelInfos = searchInfoObjectsGen(withLinkTo = project).generateList()
        val tsInfos = modelInfos
          .map { modelInfo =>
            searchInfoObjectsGen(withLinkTo = project).generateOne
              .copy(topmostSameAs = modelInfo.topmostSameAs)
          }

        givenSearchInfoFetcher(project, returning = Random.shuffle(tsInfos).pure[Try])

        commandsProducer.toUpdateCommands(project)(modelInfos).map(_.toSet) shouldBe (modelInfos zip tsInfos)
          .map { case (modelInfo, tsInfo) => CalculatorInfoSet(project, modelInfo.some, tsInfo.some) }
          .map(calculateCommands(_))
          .sequence
          .map(_.flatten.toSet)
      }

    "produce commands - " +
      "case when not all model infos have counterparts in the TS" in new TestCase {

        val modelInfos = searchInfoObjectsGen(withLinkTo = project).generateList(min = 1)
        val tsInfos    = modelInfos.tail

        givenSearchInfoFetcher(project, returning = Random.shuffle(tsInfos).pure[Try])

        commandsProducer.toUpdateCommands(project)(modelInfos).map(_.toSet) shouldBe
          (modelInfos.map(_.some) zip (None :: tsInfos.map(_.some)))
            .map { case (maybeModelInfo, maybeTsInfo) => CalculatorInfoSet(project, maybeModelInfo, maybeTsInfo) }
            .map(calculateCommands(_))
            .sequence
            .map(_.flatten.toSet)
      }

    "produce commands - " +
      "case when not all TS infos have counterparts in the model" in new TestCase {

        val tsInfos    = searchInfoObjectsGen(withLinkTo = project).generateList(min = 1)
        val modelInfos = tsInfos.tail

        givenSearchInfoFetcher(project, returning = Random.shuffle(tsInfos).pure[Try])

        commandsProducer.toUpdateCommands(project)(modelInfos).map(_.toSet) shouldBe
          ((None :: modelInfos.map(_.some)) zip tsInfos.map(_.some))
            .map { case (maybeModelInfo, maybeTsInfo) => CalculatorInfoSet(project, maybeModelInfo, maybeTsInfo) }
            .map(calculateCommands(_))
            .sequence
            .map(_.flatten.toSet)
      }
  }

  private trait TestCase {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    private val searchInfoFetcher = mock[SearchInfoFetcher[Try]]
    val commandsProducer          = new UpdateCommandsProducerImpl[Try](searchInfoFetcher)

    def givenSearchInfoFetcher(project: entities.Project, returning: Try[List[SearchInfo]]) =
      (searchInfoFetcher.fetchTSSearchInfos _)
        .expects(project.resourceId)
        .returning(returning)
  }
}
