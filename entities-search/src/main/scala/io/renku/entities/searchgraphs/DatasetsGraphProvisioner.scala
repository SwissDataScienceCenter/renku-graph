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

import cats.MonadThrow
import cats.syntax.all._
import commands.UpdateCommandsProducer
import io.renku.graph.model.entities.Project

trait DatasetsGraphProvisioner[F[_]] {
  def provisionDatasetsGraph(project: Project): F[Unit]
}

private class DatasetsGraphProvisionerImpl[F[_]: MonadThrow](updatesProducer: UpdateCommandsProducer[F],
                                                             searchInfoUploader: SearchInfoUploader[F]
) extends DatasetsGraphProvisioner[F] {

  import DatasetsCollector.collectLastVersions
  import SearchInfoExtractor.extractSearchInfo
  import updatesProducer.toUpdateCommands

  override def provisionDatasetsGraph(project: Project): F[Unit] =
    collectLastVersions
      .andThen(extractSearchInfo[F](project))
      .andThenF(toUpdateCommands(project))(project)
      .flatMap(searchInfoUploader.upload)
}
