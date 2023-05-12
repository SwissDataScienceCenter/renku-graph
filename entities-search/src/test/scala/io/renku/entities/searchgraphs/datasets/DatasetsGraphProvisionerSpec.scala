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

import cats.syntax.all._
import io.renku.entities.searchgraphs.datasets.DatasetsCollector._
import io.renku.entities.searchgraphs.datasets.Generators.updateCommands
import io.renku.entities.searchgraphs.datasets.SearchInfoExtractor._
import io.renku.entities.searchgraphs.datasets.commands.{UpdateCommand, UpdateCommandsProducer}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, positiveInts}
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class DatasetsGraphProvisionerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "provisionDatasetsGraph" should {

    "collect all the Datasets that are the latest modifications and not invalidations, " +
      "extract the Datasets graph relevant data " +
      "produce TS update commands and " +
      "push the commands into the TS" in new TestCase {

        val project = anyRenkuProjectEntities
          .withDatasets(
            List.fill(positiveInts(max = 5).generateOne.value)(datasetEntities(provenanceNonModified)): _*
          )
          .generateOne
          .to[entities.Project]

        val Success(searchInfos) = givenSearchInfoExtraction(project)

        val updates = updateCommands.generateList()
        givenUpdatesProducing(project.identification, searchInfos, returning = updates.pure[Try])

        givenUploading(updates, returning = ().pure[Try])

        provisioner.provisionDatasetsGraph(project) shouldBe ().pure[Try]
      }

    "fail if updates producing step fails" in new TestCase {

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]

      val Success(searchInfos) = givenSearchInfoExtraction(project)

      val failure = exceptions.generateOne.raiseError[Try, List[UpdateCommand]]
      givenUpdatesProducing(project.identification, searchInfos, returning = failure)

      provisioner.provisionDatasetsGraph(project) shouldBe failure
    }

    "fail if updates uploading fails" in new TestCase {

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]

      val Success(searchInfos) = givenSearchInfoExtraction(project)

      val updates = updateCommands.generateList()
      givenUpdatesProducing(project.identification, searchInfos, returning = updates.pure[Try])

      val failure = exceptions.generateOne.raiseError[Try, Unit]
      givenUploading(updates, returning = failure)

      provisioner.provisionDatasetsGraph(project) shouldBe failure
    }
  }

  private trait TestCase {
    private val commandsProducer = mock[UpdateCommandsProducer[Try]]
    private val commandsUploader = mock[UpdateCommandsUploader[Try]]
    val provisioner              = new DatasetsGraphProvisionerImpl[Try](commandsProducer, commandsUploader)

    def givenSearchInfoExtraction(project: entities.Project): Try[List[DatasetSearchInfo]] =
      (collectLastVersions >>> extractSearchInfo[Try](project))(project)

    def givenUpdatesProducing(project:     entities.ProjectIdentification,
                              searchInfos: List[DatasetSearchInfo],
                              returning:   Try[List[UpdateCommand]]
    ) = (commandsProducer
      .toUpdateCommands(_: entities.ProjectIdentification)(_: List[DatasetSearchInfo]))
      .expects(project, searchInfos)
      .returning(returning)

    def givenUploading(commands: List[UpdateCommand], returning: Try[Unit]) =
      (commandsUploader.upload _)
        .expects(commands)
        .returning(returning)
  }
}
