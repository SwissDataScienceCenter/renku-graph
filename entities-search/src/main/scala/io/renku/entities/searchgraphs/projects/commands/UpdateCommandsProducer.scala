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

package io.renku.entities.searchgraphs.projects
package commands

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.entities.searchgraphs.UpdateCommand
import io.renku.entities.searchgraphs.projects.ProjectSearchInfo
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private[projects] trait UpdateCommandsProducer[F[_]] {
  def toUpdateCommands(modelInfo: ProjectSearchInfo): F[List[UpdateCommand]]
}

private[projects] object UpdateCommandsProducer {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): UpdateCommandsProducer[F] =
    new UpdateCommandsProducerImpl[F](SearchInfoFetcher[F](connectionConfig), CommandsCalculator[F])
}

private class UpdateCommandsProducerImpl[F[_]: MonadThrow](searchInfoFetcher: SearchInfoFetcher[F],
                                                           commandsCalculator: CommandsCalculator[F]
) extends UpdateCommandsProducer[F] {

  override def toUpdateCommands(modelInfo: ProjectSearchInfo): F[List[UpdateCommand]] =
    searchInfoFetcher
      .fetchTSSearchInfo(modelInfo.id)
      .flatMap(commandsCalculator.calculateCommands(modelInfo, _))
}
