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

import SearchInfoLens.updateLinkProject
import TSDatasetSearchInfo.tsSearchInfoLinks
import cats.syntax.all._
import io.renku.entities.searchgraphs.Generators.updateCommands
import io.renku.entities.searchgraphs.UpdateCommand
import io.renku.entities.searchgraphs.datasets.Generators.{linkObjectsGen, modelDatasetSearchInfoObjects}
import io.renku.entities.searchgraphs.datasets.ModelDatasetSearchInfo
import io.renku.entities.searchgraphs.datasets.commands.CalculatorInfoSetGenerators.tsDatasetSearchInfoObjects
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.util.{Random, Try}

class UpdateCommandsProducerSpec extends AnyFlatSpec with should.Matchers with MockFactory with TryValues {

  it should "fetch Datasets currently associated with the given Project in the TS, " +
    "zip them with the datasets found on the Project and generate update commands for them " +
    "- case when all the model infos have counterparts in the TS " +
    "and there is the same and sole project only on the infos in TS" in {

      val project    = anyProjectEntities.generateOne.to[entities.Project]
      val modelInfos = modelDatasetSearchInfoObjects(withLinkTo = project).generateList(min = 1)
      val tsInfos =
        modelInfos.map(info => tsDatasetSearchInfoObjects(withLinkTo = project, info.topmostSameAs).generateOne)

      givenTSInfoFetcher(project, returning = Random.shuffle(tsInfos).pure[Try])

      val expectedCommands =
        toInfoSets(project, modelInfos.map(Option(_)), tsInfos.map(Option(_)))
          .flatMap(givenCommandsCalculation(_, returning = updateCommands.generateList()))

      commandsProducer.toUpdateCommands(project.identification)(modelInfos).map(_.toSet).success.value shouldBe
        expectedCommands.toSet
    }

  it should "fetch Datasets currently associated with the given Project in the TS, " +
    "zip them with the datasets found on the Project and generate update commands for them " +
    "- case when infos in the TS has more or different projects" in {

      val project    = anyProjectEntities.generateOne.to[entities.Project]
      val modelInfo1 = modelDatasetSearchInfoObjects(withLinkTo = project).generateOne
      val modelInfo2 = modelDatasetSearchInfoObjects(withLinkTo = project).generateOne
      val modelInfos = List(modelInfo1, modelInfo2)

      val tsInfo1Project = anyProjectEntities.generateOne.to[entities.Project]
      val tsInfo1        = tsDatasetSearchInfoObjects(withLinkTo = tsInfo1Project, modelInfo1.topmostSameAs).generateOne
      val tsInfo2Project = anyProjectEntities.generateOne.to[entities.Project]
      val tsInfo2 =
        tsDatasetSearchInfoObjects(withLinkTo = project, modelInfo2.topmostSameAs).map { info =>
          val linkToOtherProject = updateLinkProject(tsInfo2Project)(linkObjectsGen(info.topmostSameAs).generateOne)
          tsSearchInfoLinks.modify(linkToOtherProject :: _)(info)
        }.generateOne
      val tsInfos = List(tsInfo1, tsInfo2)

      givenTSInfoFetcher(project, returning = Random.shuffle(tsInfos).pure[Try])

      val expectedCommands =
        toInfoSets(project, modelInfos.map(Option(_)), tsInfos.map(Option(_)))
          .flatMap(givenCommandsCalculation(_, returning = updateCommands.generateList()))

      commandsProducer
        .toUpdateCommands(project.identification)(modelInfos)
        .map(_.toSet)
        .success
        .value shouldBe expectedCommands.toSet
    }

  it should "produce commands - " +
    "case when not all model infos have counterparts in the TS" in {

      val project    = anyProjectEntities.generateOne.to[entities.Project]
      val modelInfos = modelDatasetSearchInfoObjects(withLinkTo = project).generateList(min = 2)
      val tsInfos =
        modelInfos.tail.map(info => tsDatasetSearchInfoObjects(withLinkTo = project, info.topmostSameAs).generateOne)

      givenTSInfoFetcher(project, returning = Random.shuffle(tsInfos).pure[Try])

      val expectedCommands =
        toInfoSets(project, modelInfos.map(_.some), None :: tsInfos.map(_.some))
          .flatMap(givenCommandsCalculation(_, returning = updateCommands.generateList()))

      commandsProducer.toUpdateCommands(project.identification)(modelInfos).map(_.toSet).success.value shouldBe
        expectedCommands.toSet
    }

  it should "produce commands - " +
    "case when not all TS infos have counterparts in the model" in {

      val project           = anyProjectEntities.generateOne.to[entities.Project]
      val initialModelInfos = modelDatasetSearchInfoObjects(withLinkTo = project).generateList(min = 2)
      val modelInfos        = initialModelInfos.tail
      val tsInfos =
        initialModelInfos.map(info => tsDatasetSearchInfoObjects(withLinkTo = project, info.topmostSameAs).generateOne)

      givenTSInfoFetcher(project, returning = Random.shuffle(tsInfos).pure[Try])

      val expectedCommands =
        toInfoSets(project, None :: modelInfos.map(_.some), tsInfos.map(_.some))
          .flatMap(givenCommandsCalculation(_, returning = updateCommands.generateList()))

      commandsProducer.toUpdateCommands(project.identification)(modelInfos).map(_.toSet).success.value shouldBe
        expectedCommands.toSet
    }

  private val calculateCommand = mockFunction[CalculatorInfoSet, List[UpdateCommand]]
  private val tsInfoFetcher    = mock[TSSearchInfoFetcher[Try]]
  private lazy val commandsProducer = new UpdateCommandsProducerImpl[Try](
    tsInfoFetcher,
    new CommandsCalculator {
      override def calculateCommands: CalculatorInfoSet => List[UpdateCommand] = calculateCommand
    }
  )

  private def givenTSInfoFetcher(project: entities.Project, returning: Try[List[TSDatasetSearchInfo]]) =
    (tsInfoFetcher.fetchTSSearchInfos _)
      .expects(project.resourceId)
      .returning(returning)

  private def givenCommandsCalculation(infoSet: CalculatorInfoSet, returning: List[UpdateCommand]) = {
    calculateCommand.expects(infoSet).returning(returning)
    returning
  }

  private def toInfoSets(project:    entities.Project,
                         modelInfos: List[Option[ModelDatasetSearchInfo]],
                         tsInfos:    List[Option[TSDatasetSearchInfo]]
  ): Seq[CalculatorInfoSet] =
    (modelInfos zip tsInfos)
      .map { case (maybeModelInfo, maybeTsInfo) =>
        CalculatorInfoSet
          .from(project.identification, maybeModelInfo, maybeTsInfo)
          .fold(throw _, identity)
      }
}
