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

package io.renku.triplesgenerator.events.categories.triplesgenerated.projectinfo

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.circe.Decoder
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.{GitLabApiUrl, projects, users}
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import org.http4s.{EntityDecoder, InvalidMessageBodyFailure, Request, Response, Status}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

private trait CommitAuthorFinder[F[_]] {
  def findCommitAuthor(projectPath: projects.Path, commitId: CommitId)(implicit
      maybeAccessToken:             Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Option[(users.Name, users.Email)]]
}

private object CommitAuthorFinder {
  def apply[F[_]: Async: Logger](gitLabThrottler: Throttler[F, GitLab]): F[CommitAuthorFinder[F]] = for {
    gitLabUrl <- GitLabUrlLoader[F]()
  } yield new CommitAuthorFinderImpl(gitLabUrl.apiV4, gitLabThrottler)
}

private class CommitAuthorFinderImpl[F[_]: Async: Logger](
    gitLabApiUrl:           GitLabApiUrl,
    gitLabThrottler:        Throttler[F, GitLab],
    recoveryStrategy:       RecoverableErrorsRecovery = RecoverableErrorsRecovery,
    retryInterval:          FiniteDuration = RestClient.SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient(gitLabThrottler,
                     retryInterval = retryInterval,
                     maxRetries = maxRetries,
                     requestTimeoutOverride = requestTimeoutOverride
    )
    with CommitAuthorFinder[F] {

  import org.http4s.Method.GET
  import org.http4s.dsl.io.{NotFound, Ok}

  override def findCommitAuthor(path: projects.Path, commitId: CommitId)(implicit
      maybeAccessToken:               Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Option[(users.Name, users.Email)]] = EitherT {
    {
      for {
        projectsUri <- validateUri(s"$gitLabApiUrl/projects/${urlEncode(path.value)}/repository/commits/$commitId")
        maybeAuthor <- send(secureRequest(GET, projectsUri))(mapTo[(users.Name, users.Email)])
      } yield maybeAuthor
    }.map(_.asRight[ProcessingRecoverableError]).recoverWith(recoveryStrategy.maybeRecoverableError)
  }

  private def mapTo[OUT](implicit
      decoder: EntityDecoder[F, OUT]
  ): PartialFunction[(Status, Request[F], Response[F]), F[Option[OUT]]] = {
    case (Ok, _, response) => response.as[OUT].map(Option.apply).recoverWith(noAuthor[OUT])
    case (NotFound, _, _)  => Option.empty[OUT].pure[F]
  }

  private implicit lazy val authorDecoder: EntityDecoder[F, (users.Name, users.Email)] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._
    import org.http4s.circe.jsonOf

    implicit val decoder: Decoder[(users.Name, users.Email)] = cursor =>
      (
        cursor.downField("author_name").as[users.Name],
        cursor.downField("author_email").as[users.Email]
      ).mapN(_ -> _)

    jsonOf[F, (users.Name, users.Email)]
  }

  private def noAuthor[OUT]: PartialFunction[Throwable, F[Option[OUT]]] = { case _: InvalidMessageBodyFailure =>
    Option.empty[OUT].pure[F]
  }
}
