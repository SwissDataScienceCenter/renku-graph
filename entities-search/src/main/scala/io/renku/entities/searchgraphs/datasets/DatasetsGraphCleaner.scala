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

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.entities.searchgraphs.UpdateCommandsUploader
import io.renku.entities.searchgraphs.datasets.commands.UpdateCommandsProducer
import io.renku.graph.model.entities.ProjectIdentification
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

trait DatasetsGraphCleaner[F[_]] {
  def cleanDatasetsGraph(project: ProjectIdentification): F[Unit]
}

object DatasetsGraphCleaner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): DatasetsGraphCleaner[F] = {
    val updatesProducer    = UpdateCommandsProducer[F](connectionConfig)
    val searchInfoUploader = UpdateCommandsUploader[F](connectionConfig)
    new DatasetsGraphCleanerImpl[F](updatesProducer, searchInfoUploader)
  }

  def default[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[DatasetsGraphCleaner[F]] =
    ProjectsConnectionConfig[F]().map(apply(_))
}

private class DatasetsGraphCleanerImpl[F[_]: MonadThrow](updatesProducer: UpdateCommandsProducer[F],
                                                         searchInfoUploader: UpdateCommandsUploader[F]
) extends DatasetsGraphCleaner[F] {

  import updatesProducer.toUpdateCommands

  override def cleanDatasetsGraph(project: ProjectIdentification): F[Unit] =
    toUpdateCommands(project)(Nil) >>= searchInfoUploader.upload
}
