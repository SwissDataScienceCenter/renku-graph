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

import Generators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.entities.searchgraphs.Generators.updateCommands
import io.renku.entities.searchgraphs.datasets.DatasetsCollector._
import io.renku.entities.searchgraphs.datasets.ModelSearchInfoExtractor._
import io.renku.entities.searchgraphs.datasets.commands.UpdateCommandsProducer
import io.renku.entities.searchgraphs.{UpdateCommand, UpdateCommandsUploader}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.positiveInts
import io.renku.graph.model.testentities._
import io.renku.graph.model.{datasets, entities}
import io.renku.lock.Lock
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class DatasetsGraphProvisionerSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers with AsyncMockFactory {

  "provisionDatasetsGraph" should {

    "collect all the Datasets that are the latest modifications and not invalidations, " +
      "extract the Datasets graph relevant data," +
      "fetch from TS datasets for the project, " +
      "produce TS update commands and push them to the TS for each found DS" in {

        val project = anyRenkuProjectEntities
          .withDatasets(List.fill(positiveInts(max = 5).generateOne.value)(datasetEntities(provenanceNonModified)): _*)
          .generateOne
          .to[entities.Project]

        for {
          // updates for datasets found in model
          modelSearchInfos <- givenSearchInfoExtraction(project)
          _ = modelSearchInfos foreach { mi =>
                val updates = updateCommands.generateList()
                givenUpdatesProducing(project.identification, mi, returning = updates.pure[IO])
                givenUploading(updates, returning = ().pure[IO])
              }

          // updates for datasets in TS
          tsOnlyInfos = tsDatasetSearchInfoObjects(project).generateList(min = 1)
          modelAndTSInfo =
            tsDatasetSearchInfoObjects(project, project.datasets.head.provenance.topmostSameAs).generateOne
          _ = givenTSSearchInfosForProject(project.identification, returning = (modelAndTSInfo :: tsOnlyInfos).pure[IO])
          _ = tsOnlyInfos foreach { tsi =>
                val updates = updateCommands.generateList()
                givenUpdatesProducing(project.identification, tsi, returning = updates.pure[IO])
                givenUploading(updates, returning = ().pure[IO])
              }

          result <- provisioner.provisionDatasetsGraph(project).assertNoException
        } yield result
      }
  }

  private val topSameAsLock: Lock[IO, datasets.TopmostSameAs] = Lock.none[IO, datasets.TopmostSameAs]
  private val commandsProducer    = mock[UpdateCommandsProducer[IO]]
  private val commandsUploader    = mock[UpdateCommandsUploader[IO]]
  private val tsSearchInfoFetcher = mock[TSSearchInfoFetcher[IO]]
  private lazy val provisioner =
    new DatasetsGraphProvisionerImpl[IO](tsSearchInfoFetcher, commandsProducer, commandsUploader, topSameAsLock)

  private def givenSearchInfoExtraction(project: entities.Project): IO[List[ModelDatasetSearchInfo]] =
    (collectLastVersions >>> extractModelSearchInfos[IO](project))(project)

  private def givenTSSearchInfosForProject(project:   entities.ProjectIdentification,
                                           returning: IO[List[TSDatasetSearchInfo]]
  ) = (tsSearchInfoFetcher.findTSInfosByProject _)
    .expects(project.resourceId)
    .returning(returning)

  private def givenUpdatesProducing(project:    entities.ProjectIdentification,
                                    searchInfo: ModelDatasetSearchInfo,
                                    returning:  IO[List[UpdateCommand]]
  ) = (commandsProducer
    .toUpdateCommands(_: entities.ProjectIdentification, _: ModelDatasetSearchInfo))
    .expects(project, searchInfo)
    .returning(returning)

  private def givenUpdatesProducing(project:    entities.ProjectIdentification,
                                    searchInfo: TSDatasetSearchInfo,
                                    returning:  IO[List[UpdateCommand]]
  ) = (commandsProducer
    .toUpdateCommands(_: entities.ProjectIdentification, _: TSDatasetSearchInfo))
    .expects(project, searchInfo)
    .returning(returning)

  private def givenUploading(commands: List[UpdateCommand], returning: IO[Unit]) =
    (commandsUploader.upload _)
      .expects(commands)
      .returning(returning)
}
