/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.tokenrepository.repository.deletion

import cats.MonadError
import cats.effect.{ContextShift, Effect, IO}
import cats.syntax.all._
import ch.datascience.http.ErrorMessage._
import ch.datascience.db.{SessionResource, SqlStatement}
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.ErrorMessage
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.ProjectsTokensDB
import org.typelevel.log4cats.Logger
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

import scala.util.control.NonFatal

class DeleteTokenEndpoint[Interpretation[_]: Effect](
    tokenRemover: TokenRemover[Interpretation],
    logger:       Logger[Interpretation]
)(implicit ME:    MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  def deleteToken(projectId: Id): Interpretation[Response[Interpretation]] =
    tokenRemover
      .delete(projectId)
      .flatMap(_ => NoContent())
      .recoverWith(httpResult(projectId))

  private def httpResult(projectId: Id): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage(s"Deleting token for projectId: $projectId failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }
}

object IODeleteTokenEndpoint {
  def apply(
      sessionResource:     SessionResource[IO, ProjectsTokensDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlStatement.Name],
      logger:              Logger[IO]
  )(implicit contextShift: ContextShift[IO]): IO[DeleteTokenEndpoint[IO]] = IO {
    new DeleteTokenEndpoint[IO](new TokenRemover[IO](sessionResource, queriesExecTimes), logger)
  }
}
