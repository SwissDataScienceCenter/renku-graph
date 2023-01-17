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

import Generators.{searchInfoObjectsGen, updateCommands}
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{projectResourceIds, projectVisibilities}
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Random, Try}

class UpdateCommandsProducerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "toUpdateCommands" should {

    "fetch Datasets currently associated with the given Project in the TS, " +
      "zip them with the datasets found on the Project and generate update commands for them " +
      "- case when all the model infos have counterparts in the TS " +
      "and there is the same and sole project only on the infos in TS" in new TestCase {

        val modelInfos = searchInfoObjectsGen(withLinkTo = project).generateList(min = 1)
        val tsInfos    = modelInfos

        givenSearchInfoFetcher(project, returning = Random.shuffle(tsInfos).pure[Try])

        givenProjectVisibilityFinder(project.resourceId, returning = project.visibility.some.pure[Try])

        val expectedCommands =
          toInfoSets(modelInfos.map(Option(_)), tsInfos.map(Option(_)), Map(project.resourceId -> project.visibility))
            .map(givenCommandsCalculation(_, returning = updateCommands.generateList().pure[Try]))
            .sequence
            .map(_.flatten)

        commandsProducer.toUpdateCommands(project.identification)(modelInfos).map(_.toSet) shouldBe
          expectedCommands.map(_.toSet)
      }

    "fetch Datasets currently associated with the given Project in the TS, " +
      "zip them with the datasets found on the Project and generate update commands for them " +
      "- case when infos in the TS has more or different projects" in new TestCase {

        val modelInfo1 = searchInfoObjectsGen(withLinkTo = project).generateOne
        val modelInfo2 = searchInfoObjectsGen(withLinkTo = project).generateOne
        val modelInfos = List(modelInfo1, modelInfo2)

        val tsInfo1Project = projectResourceIds.generateOne -> projectVisibilities.generateOne
        val tsInfo1 =
          searchInfoObjectsGen(withLinkTo = tsInfo1Project._1).generateOne
            .copy(topmostSameAs = modelInfo1.topmostSameAs)
        val tsInfo2Project = projectResourceIds.generateOne -> projectVisibilities.generateOne
        val tsInfo2 =
          searchInfoObjectsGen(withLinkTo = project.resourceId, and = tsInfo2Project._1).generateOne
            .copy(topmostSameAs = modelInfo2.topmostSameAs)
        val tsInfos = List(tsInfo1, tsInfo2)

        givenSearchInfoFetcher(project, returning = Random.shuffle(tsInfos).pure[Try])

        givenProjectVisibilityFinder(project.resourceId, returning = project.visibility.some.pure[Try])
        givenProjectVisibilityFinder(tsInfo1Project._1, returning = tsInfo1Project._2.some.pure[Try])
        givenProjectVisibilityFinder(tsInfo2Project._1, returning = tsInfo2Project._2.some.pure[Try])

        val expectedCommands =
          toInfoSets(modelInfos.map(Option(_)),
                     tsInfos.map(Option(_)),
                     Map(project.resourceId -> project.visibility) + tsInfo1Project + tsInfo2Project
          )
            .map(givenCommandsCalculation(_, returning = updateCommands.generateList().pure[Try]))
            .sequence
            .map(_.flatten)

        commandsProducer.toUpdateCommands(project.identification)(modelInfos).map(_.toSet) shouldBe expectedCommands
          .map(_.toSet)
      }

    "produce commands - " +
      "case when not all model infos have counterparts in the TS" in new TestCase {

        val modelInfos = searchInfoObjectsGen(withLinkTo = project).generateList(min = 2)
        val tsInfos    = modelInfos.tail

        givenSearchInfoFetcher(project, returning = Random.shuffle(tsInfos).pure[Try])

        givenProjectVisibilityFinder(project.resourceId, returning = project.visibility.some.pure[Try])

        val expectedCommands =
          toInfoSets(modelInfos.map(_.some), None :: tsInfos.map(_.some), Map(project.resourceId -> project.visibility))
            .map(givenCommandsCalculation(_, returning = updateCommands.generateList().pure[Try]))
            .sequence
            .map(_.flatten)

        commandsProducer.toUpdateCommands(project.identification)(modelInfos).map(_.toSet) shouldBe
          expectedCommands.map(_.toSet)
      }

    "produce commands - " +
      "case when not all TS infos have counterparts in the model" in new TestCase {

        val tsInfos    = searchInfoObjectsGen(withLinkTo = project).generateList(min = 1)
        val modelInfos = tsInfos.tail

        givenSearchInfoFetcher(project, returning = Random.shuffle(tsInfos).pure[Try])

        givenProjectVisibilityFinder(project.resourceId, returning = project.visibility.some.pure[Try])

        val expectedCommands =
          toInfoSets(None :: modelInfos.map(_.some), tsInfos.map(_.some), Map(project.resourceId -> project.visibility))
            .map(givenCommandsCalculation(_, returning = updateCommands.generateList().pure[Try]))
            .sequence
            .map(_.flatten)

        commandsProducer.toUpdateCommands(project.identification)(modelInfos).map(_.toSet) shouldBe expectedCommands
          .map(_.toSet)
      }
  }

  private trait TestCase {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    private val calculateCommand  = mockFunction[CalculatorInfoSet, Try[List[UpdateCommand]]]
    private val searchInfoFetcher = mock[SearchInfoFetcher[Try]]
    private val visibilityFinder  = mock[VisibilityFinder[Try]]
    val commandsProducer = new UpdateCommandsProducerImpl[Try](
      searchInfoFetcher,
      visibilityFinder,
      new CommandsCalculator[Try] {
        override def calculateCommands: CalculatorInfoSet => Try[List[UpdateCommand]] = calculateCommand
      }
    )

    def givenSearchInfoFetcher(project: entities.Project, returning: Try[List[SearchInfo]]) =
      (searchInfoFetcher.fetchTSSearchInfos _)
        .expects(project.resourceId)
        .returning(returning)

    def givenProjectVisibilityFinder(projectId: projects.ResourceId, returning: Try[Option[projects.Visibility]]) =
      (visibilityFinder.findVisibility _)
        .expects(projectId)
        .returning(returning)

    def givenCommandsCalculation(infoSet: CalculatorInfoSet, returning: Try[List[UpdateCommand]]) = {
      calculateCommand.expects(infoSet).returning(returning)
      returning
    }

    def toInfoSets(modelInfos:     List[Option[SearchInfo]],
                   tsInfos:        List[Option[SearchInfo]],
                   tsVisibilities: Map[projects.ResourceId, projects.Visibility]
    ): Seq[CalculatorInfoSet] =
      (modelInfos zip tsInfos)
        .map { case (maybeModelInfo, maybeTsInfo) =>
          CalculatorInfoSet
            .from(project.identification, maybeModelInfo, maybeTsInfo, tsVisibilities)
            .fold(throw _, identity)
        }
  }
}
