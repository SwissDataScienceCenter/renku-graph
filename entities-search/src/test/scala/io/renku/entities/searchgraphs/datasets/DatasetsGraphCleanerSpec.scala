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

package io.renku.entities.searchgraphs.datasets

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.entities.searchgraphs.Generators._
import io.renku.entities.searchgraphs.datasets.Generators.tsDatasetSearchInfoObjects
import io.renku.entities.searchgraphs.datasets.commands.UpdateCommandsProducer
import io.renku.entities.searchgraphs.{UpdateCommand, UpdateCommandsUploader}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.testentities.projectIdentifications
import io.renku.graph.model.{datasets, entities}
import io.renku.lock.Lock
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class DatasetsGraphCleanerSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers with AsyncMockFactory {

  "cleanDatasetsGraph" should {

    "fetch datasets infos from TS," +
      "produce TS update commands and " +
      "push them to the TS" in {

        val project = projectIdentifications.generateOne

        val tsInfos = tsDatasetSearchInfoObjects(project.resourceId).generateList(min = 1)
        givenTSSearchInfosForProject(project, returning = tsInfos.pure[IO])

        tsInfos foreach { tsInfo =>
          val updates = updateCommands.generateList(min = 1)
          givenUpdatesProducing(project, tsInfo, returning = updates.pure[IO])

          givenUploading(updates, returning = ().pure[IO])
        }

        cleaner.cleanDatasetsGraph(project).assertNoException
      }

    "fail if updates producing step fails" in {

      val project = projectIdentifications.generateOne

      val tsInfo = tsDatasetSearchInfoObjects(project.resourceId).generateOne
      givenTSSearchInfosForProject(project, returning = List(tsInfo).pure[IO])

      val failure = exceptions.generateOne
      givenUpdatesProducing(project, tsInfo, returning = failure.raiseError[IO, List[UpdateCommand]])

      cleaner.cleanDatasetsGraph(project).assertThrowsError[Exception](_ shouldBe failure)
    }

    "fail if updates uploading fails" in {

      val project = projectIdentifications.generateOne

      val tsInfo = tsDatasetSearchInfoObjects(project.resourceId).generateOne
      givenTSSearchInfosForProject(project, returning = List(tsInfo).pure[IO])

      val updates = updateCommands.generateList(min = 1)
      givenUpdatesProducing(project, tsInfo, returning = updates.pure[IO])
      val failure = exceptions.generateOne
      givenUploading(updates, returning = failure.raiseError[IO, Unit])

      cleaner.cleanDatasetsGraph(project).assertThrowsError[Exception](_ shouldBe failure)
    }
  }

  private val tsSearchInfoFetcher = mock[TSSearchInfoFetcher[IO]]
  private val commandsProducer    = mock[UpdateCommandsProducer[IO]]
  private val commandsUploader    = mock[UpdateCommandsUploader[IO]]
  private val topSameAsLock: Lock[IO, datasets.TopmostSameAs] = Lock.none[IO, datasets.TopmostSameAs]
  private lazy val cleaner =
    new DatasetsGraphCleanerImpl[IO](tsSearchInfoFetcher, commandsProducer, commandsUploader, topSameAsLock)

  private def givenTSSearchInfosForProject(project:   entities.ProjectIdentification,
                                           returning: IO[List[TSDatasetSearchInfo]]
  ) = (tsSearchInfoFetcher.findTSInfosByProject _)
    .expects(project.resourceId)
    .returning(returning)

  private def givenUpdatesProducing(project:   entities.ProjectIdentification,
                                    tsInfo:    TSDatasetSearchInfo,
                                    returning: IO[List[UpdateCommand]]
  ) = (commandsProducer
    .toUpdateCommands(_: entities.ProjectIdentification, _: TSDatasetSearchInfo))
    .expects(project, tsInfo)
    .returning(returning)

  private def givenUploading(commands: List[UpdateCommand], returning: IO[Unit]) =
    (commandsUploader.upload _)
      .expects(commands)
      .returning(returning)
}
