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
import io.renku.entities.searchgraphs.datasets.commands.UpdateCommand
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait UpdateCommandsUploader[F[_]] {
  def upload(commands: List[UpdateCommand]): F[Unit]
}

private object UpdateCommandsUploader {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): UpdateCommandsUploader[F] =
    new UpdateCommandsUploaderImpl[F](TSClient[F](connectionConfig))

  def default[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[UpdateCommandsUploader[F]] =
    ProjectsConnectionConfig[F]().map(apply(_))
}

private class UpdateCommandsUploaderImpl[F[_]: MonadThrow](tsClient: TSClient[F]) extends UpdateCommandsUploader[F] {

  import eu.timepit.refined.auto._
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.client.syntax._
  import tsClient.updateWithNoResult

  override def upload(commands: List[UpdateCommand]): F[Unit] =
    commands
      .groupBy(commandType)
      .map(toSparqlQueries)
      .toList
      .map(updateWithNoResult)
      .sequence
      .void

  private sealed trait CommandType extends Product with Serializable
  private object CommandType {
    final case object Insert extends CommandType
    final case object Delete extends CommandType
  }

  private lazy val commandType: UpdateCommand => CommandType = {
    case _: UpdateCommand.Insert => CommandType.Insert
    case _: UpdateCommand.Delete => CommandType.Delete
  }

  private lazy val toSparqlQueries: ((CommandType, List[UpdateCommand])) => SparqlQuery = {
    case (CommandType.Insert, cmds) =>
      SparqlQuery.of("search info inserts", s"INSERT DATA {\n${cmds.map(_.quad.asSparql).combineAll.sparql}\n}")
    case (CommandType.Delete, cmds) =>
      SparqlQuery.of("search info deletes", s"DELETE DATA {\n${cmds.map(_.quad.asSparql).combineAll.sparql}\n}")
  }
}
