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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.data.EitherT
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabApiUrl
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import ch.datascience.http.client.RestClientError.ConnectivityException
import ch.datascience.http.client.{AccessToken, IORestClient}
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.IOTriplesCurator.CurationRecoverableError
import org.http4s.Method.GET
import org.http4s.Status.{Ok, Unauthorized}
import org.http4s.circe.jsonOf
import org.http4s.dsl.io.ServiceUnavailable
import org.http4s.{EntityDecoder, Request, Response, Status}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait CommitCommitterFinder[Interpretation[_]] {

  def findCommitPeople(projectId:        projects.Id,
                       commitId:         CommitId,
                       maybeAccessToken: Option[AccessToken]
  ): EitherT[Interpretation, ProcessingRecoverableError, CommitPersonsInfo]
}

private class CommitCommitterFinderImpl(
    gitLabApiUrl:    GitLabApiUrl,
    gitLabThrottler: Throttler[IO, GitLab],
    logger:          Logger[IO]
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends IORestClient(gitLabThrottler, logger)
    with CommitCommitterFinder[IO] {

  def findCommitPeople(projectId:        projects.Id,
                       commitId:         CommitId,
                       maybeAccessToken: Option[AccessToken]
  ): EitherT[IO, ProcessingRecoverableError, CommitPersonsInfo] =
    for {
      uri    <- validateUri(s"$gitLabApiUrl/projects/$projectId/repository/commits/$commitId").toRightT
      result <- EitherT(send(request(GET, uri, maybeAccessToken))(mapResponse) recoverWith maybeRecoverableError)
    } yield result

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[
    Either[ProcessingRecoverableError, CommitPersonsInfo]
  ]] = {
    case (Ok, _, response) => response.as[CommitPersonsInfo].map(info => Right(info))
    case (ServiceUnavailable, _, _) =>
      Left(CurationRecoverableError("Service unavailable")).pure[IO]
    case (Unauthorized, _, _) =>
      Left(CurationRecoverableError("Access token not valid to fetch project commit info")).pure[IO]
  }

  private implicit val commitPersonInfoEntityDecoder: EntityDecoder[IO, CommitPersonsInfo] =
    jsonOf[IO, CommitPersonsInfo]

  private lazy val maybeRecoverableError
      : PartialFunction[Throwable, IO[Either[ProcessingRecoverableError, CommitPersonsInfo]]] = {
    case ConnectivityException(message, cause) =>
      IO.pure(Either.left(CurationRecoverableError(message, cause)))
  }

  private implicit class ResultOps[T](out: IO[T]) {
    lazy val toRightT: EitherT[IO, ProcessingRecoverableError, T] =
      EitherT.right[ProcessingRecoverableError](out)
  }

}

private object IOCommitCommitterFinder {
  def apply(gitLabApiUrl: GitLabApiUrl, gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO])(implicit
      executionContext:   ExecutionContext,
      contextShift:       ContextShift[IO],
      timer:              Timer[IO]
  ): IO[CommitCommitterFinder[IO]] = IO(new CommitCommitterFinderImpl(gitLabApiUrl, gitLabThrottler, logger))
}
