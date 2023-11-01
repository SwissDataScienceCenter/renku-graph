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

package io.renku.entities.searchgraphs.datasets
package commands

import cats.syntax.all._
import io.renku.entities.searchgraphs.Generators.updateCommands
import io.renku.entities.searchgraphs.UpdateCommand
import io.renku.entities.searchgraphs.datasets.Generators.{modelDatasetSearchInfoObjects, tsDatasetSearchInfoObjects}
import io.renku.entities.searchgraphs.datasets.ModelDatasetSearchInfo
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.graph.model.{datasets, entities}
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class UpdateCommandsProducerSpec extends AnyWordSpec with should.Matchers with MockFactory with TryValues {

  "toUpdateCommands(ModelDatasetSearchInfo)" should {

    "find matching Info in the TS and generate update commands for the tuple" in {

      val project     = anyProjectEntities.generateOne.to[entities.Project]
      val modelInfo   = modelDatasetSearchInfoObjects(project).generateOne
      val maybeTSInfo = tsDatasetSearchInfoObjects(withLinkTo = project, modelInfo.topmostSameAs).generateSome

      givenTSFetchingByTopSameAs(modelInfo.topmostSameAs, returning = maybeTSInfo.pure[Try])

      val expectedCommands =
        givenCommandsCalculation(toInfoSet(project, modelInfo.some, maybeTSInfo),
                                 returning = updateCommands.generateList()
        )

      commandsProducer.toUpdateCommands(project.identification, modelInfo).success.value shouldBe expectedCommands
    }
  }

  "toUpdateCommands(TSDatasetSearchInfo)" should {

    "find matching Info in the TS and generate update commands for the tuple" in {

      val project = anyProjectEntities.generateOne.to[entities.Project]
      val tsInfo  = tsDatasetSearchInfoObjects(withLinkTo = project).generateOne

      val expectedCommands =
        givenCommandsCalculation(toInfoSet(project, maybeModelInfo = None, tsInfo.some),
                                 returning = updateCommands.generateList()
        )

      commandsProducer.toUpdateCommands(project.identification, tsInfo).success.value shouldBe expectedCommands
    }
  }

  private val calculateCommand = mockFunction[CalculatorInfoSet, List[UpdateCommand]]
  private val tsInfoFetcher    = mock[TSSearchInfoFetcher[Try]]
  private lazy val commandsProducer = new UpdateCommandsProducerImpl[Try](
    tsInfoFetcher,
    new CommandsCalculator {
      override def calculateCommands: CalculatorInfoSet => List[UpdateCommand] = calculateCommand
    }
  )

  private def givenTSFetchingByTopSameAs(topSameAs: datasets.TopmostSameAs,
                                         returning: Try[Option[TSDatasetSearchInfo]]
  ) = (tsInfoFetcher.findTSInfoBySameAs _)
    .expects(topSameAs)
    .returning(returning)

  private def givenCommandsCalculation(infoSet: CalculatorInfoSet, returning: List[UpdateCommand]) = {
    calculateCommand.expects(infoSet).returning(returning)
    returning
  }

  private def toInfoSet(project:        entities.Project,
                        maybeModelInfo: Option[ModelDatasetSearchInfo],
                        maybeTsInfo:    Option[TSDatasetSearchInfo]
  ): CalculatorInfoSet =
    CalculatorInfoSet
      .from(project.identification, maybeModelInfo, maybeTsInfo)
      .fold(throw _, identity)
}
