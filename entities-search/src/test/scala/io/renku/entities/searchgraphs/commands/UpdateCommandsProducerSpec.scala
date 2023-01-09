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

import scala.util.Try

class UpdateCommandsProducerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  private val calculateCommands: CalculatorInfoSet => Try[List[UpdateCommand]] =
    CommandsCalculator.calculateCommands[Try]

  "toCommands" should {

    "fetch the current state of the DS from the TS" in new TestCase {

      val searchInfos = searchInfoObjectsGen(withLinkFor = project.resourceId)
        .generateList()
        .map { modelInfo =>
          modelInfo -> searchInfoObjectsGen(withLinkFor = project.resourceId).generateOption
            .map(_.copy(topmostSameAs = modelInfo.topmostSameAs))
        }

      searchInfos foreach { case (info, maybeStoreInfo) =>
        givenSearchInfoFetcher(info, returning = maybeStoreInfo.pure[Try])
      }

      commandsProducer.toUpdateCommands(project, searchInfos.map(_._1)) shouldBe searchInfos
        .map { case (modelInfo, maybeTsInfo) => CalculatorInfoSet(project, modelInfo.some, maybeTsInfo) }
        .map(calculateCommands(_))
        .sequence
        .map(_.flatten)
    }
  }

  private trait TestCase {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    private val searchInfoFetcher = mock[SearchInfoFetcher[Try]]
    val commandsProducer          = new UpdateCommandsProducerImpl[Try](searchInfoFetcher)

    def givenSearchInfoFetcher(searchInfo: SearchInfo, returning: Try[Option[SearchInfo]]) =
      (searchInfoFetcher.fetchStoreSearchInfo _)
        .expects(searchInfo.topmostSameAs)
        .returning(returning)
  }
}
