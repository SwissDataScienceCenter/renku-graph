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
package projects

import cats.MonadThrow
import cats.effect.Async
import commands.UpdateCommandsProducer
import io.renku.graph.model.entities.Project
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

trait ProjectsGraphProvisioner[F[_]] {
  def provisionProjectsGraph(project: Project): F[Unit]
}

object ProjectsGraphProvisioner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): ProjectsGraphProvisioner[F] =
    new ProjectsGraphProvisionerImpl[F](UpdateCommandsProducer(), UpdateCommandsUploader[F](connectionConfig))
}

private class ProjectsGraphProvisionerImpl[F[_]: MonadThrow](commandsProducer: UpdateCommandsProducer,
                                                             commandsUploader: UpdateCommandsUploader[F]
) extends ProjectsGraphProvisioner[F] {

  import SearchInfoExtractor.extractSearchInfo
  import commandsProducer.toUpdateCommands
  import commandsUploader.upload

  override def provisionProjectsGraph(project: Project): F[Unit] =
    upload(toUpdateCommands(extractSearchInfo(project)))
}
