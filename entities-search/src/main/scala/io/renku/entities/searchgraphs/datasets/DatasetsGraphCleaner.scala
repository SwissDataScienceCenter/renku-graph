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

import cats.effect.Async
import cats.syntax.all._
import io.renku.entities.searchgraphs.UpdateCommandsUploader
import io.renku.entities.searchgraphs.datasets.commands.UpdateCommandsProducer
import io.renku.graph.model.datasets
import io.renku.graph.model.entities.ProjectIdentification
import io.renku.lock.Lock
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private[searchgraphs] trait DatasetsGraphCleaner[F[_]] {
  def cleanDatasetsGraph(project: ProjectIdentification): F[Unit]
}

private[searchgraphs] object DatasetsGraphCleaner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](lock:             Lock[F, datasets.TopmostSameAs],
                                                          connectionConfig: ProjectsConnectionConfig
  ): DatasetsGraphCleaner[F] = {
    val tsSearchInfoFetcher = TSSearchInfoFetcher[F](connectionConfig)
    val updatesProducer     = UpdateCommandsProducer[F](connectionConfig)
    val searchInfoUploader  = UpdateCommandsUploader[F](connectionConfig)
    new DatasetsGraphCleanerImpl[F](tsSearchInfoFetcher, updatesProducer, searchInfoUploader, lock)
  }
}

private class DatasetsGraphCleanerImpl[F[_]: Async](tsSearchInfoFetcher: TSSearchInfoFetcher[F],
                                                    updatesProducer:    UpdateCommandsProducer[F],
                                                    searchInfoUploader: UpdateCommandsUploader[F],
                                                    lock:               Lock[F, datasets.TopmostSameAs]
) extends DatasetsGraphCleaner[F] {

  import tsSearchInfoFetcher.findTSInfosByProject
  import updatesProducer.toUpdateCommands

  override def cleanDatasetsGraph(project: ProjectIdentification): F[Unit] =
    findTSInfosByProject(project.resourceId).flatMap(_.map(update(project)).sequence).void

  private def update(project: ProjectIdentification)(tsi: TSDatasetSearchInfo) =
    lock
      .run(tsi.topmostSameAs)
      .use(_ => toUpdateCommands(project, tsi).flatMap(searchInfoUploader.upload))
}
