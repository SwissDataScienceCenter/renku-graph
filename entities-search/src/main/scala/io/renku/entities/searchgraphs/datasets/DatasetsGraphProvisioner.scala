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
import io.renku.graph.model.entities.Project
import io.renku.lock.Lock
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

trait DatasetsGraphProvisioner[F[_]] {
  def provisionDatasetsGraph(project: Project): F[Unit]
}

object DatasetsGraphProvisioner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](lock:             Lock[F, datasets.TopmostSameAs],
                                                          connectionConfig: ProjectsConnectionConfig
  ): DatasetsGraphProvisioner[F] = {
    val tsSearchInfoFetcher = TSSearchInfoFetcher[F](connectionConfig)
    val updatesProducer     = UpdateCommandsProducer[F](connectionConfig)
    val searchInfoUploader  = UpdateCommandsUploader[F](connectionConfig)
    new DatasetsGraphProvisionerImpl[F](tsSearchInfoFetcher, updatesProducer, searchInfoUploader, lock)
  }

  def default[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      lock: Lock[F, datasets.TopmostSameAs]
  ): F[DatasetsGraphProvisioner[F]] =
    ProjectsConnectionConfig[F]().map(apply(lock, _))
}

private class DatasetsGraphProvisionerImpl[F[_]: Async](tsSearchInfoFetcher: TSSearchInfoFetcher[F],
                                                        updatesProducer:    UpdateCommandsProducer[F],
                                                        searchInfoUploader: UpdateCommandsUploader[F],
                                                        lock:               Lock[F, datasets.TopmostSameAs]
) extends DatasetsGraphProvisioner[F] {

  import DatasetsCollector.collectLastVersions
  import ModelSearchInfoExtractor.extractModelSearchInfos
  import updatesProducer.toUpdateCommands

  override def provisionDatasetsGraph(project: Project): F[Unit] =
    collectLastVersions
      .andThen(extractModelSearchInfos[F](project))(project)
      .flatTap(_.map(update(project, _)).sequence.void)
      .flatMap(findDatasetsNoLongerOnModel(project, _).flatMap(_.map(update(project, _)).sequence).void)

  private def findDatasetsNoLongerOnModel(project: Project, modelInfos: List[ModelDatasetSearchInfo]) =
    tsSearchInfoFetcher
      .findTSInfosByProject(project.resourceId)
      .map(_.filterNot(tsDS => modelInfos.exists(_.topmostSameAs == tsDS.topmostSameAs)))

  private def update(project: Project, mi: ModelDatasetSearchInfo) =
    lock(mi.topmostSameAs).surround(
      toUpdateCommands(project.identification, mi).flatMap(searchInfoUploader.upload)
    )

  private def update(project: Project, tsi: TSDatasetSearchInfo) =
    lock(tsi.topmostSameAs).surround(
      toUpdateCommands(project.identification, tsi).flatMap(searchInfoUploader.upload)
    )
}
