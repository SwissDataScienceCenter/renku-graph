/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.tokenrepository.repository.association

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.{RateLimit, Throttler}
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.projects
import ch.datascience.http.client.{AccessToken, IORestClient}
import io.chrisdavenport.log4cats.Logger
import org.http4s.circe.jsonOf

import scala.concurrent.ExecutionContext

trait ProjectPathFinder[Interpretation[_]] {
  def findProjectPath(
      projectId:        projects.Id,
      maybeAccessToken: Option[AccessToken]
  ): Interpretation[Option[projects.Path]]
}

private class IOProjectPathFinder(
    gitLabUrl:               GitLabUrl,
    gitLabThrottler:         Throttler[IO, GitLab],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient(gitLabThrottler, logger)
    with ProjectPathFinder[IO] {

  import cats.effect._
  import cats.syntax.all._
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe._
  import org.http4s.Method.GET
  import org.http4s.Status.Unauthorized
  import org.http4s._
  import org.http4s.dsl.io._

  def findProjectPath(projectId: projects.Id, maybeAccessToken: Option[AccessToken]): IO[Option[projects.Path]] =
    for {
      uri     <- validateUri(s"$gitLabUrl/api/v4/projects/$projectId")
      project <- send(request(GET, uri, maybeAccessToken))(mapResponse)
    } yield project

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Option[projects.Path]]] = {
    case (Ok, _, response)    => response.as[projects.Path].map(Option.apply)
    case (NotFound, _, _)     => None.pure[IO]
    case (Unauthorized, _, _) => None.pure[IO]
  }

  private implicit lazy val projectPathDecoder: EntityDecoder[IO, projects.Path] = {
    lazy val decoder: Decoder[projects.Path] = _.downField("path_with_namespace").as[projects.Path]
    jsonOf[IO, projects.Path](implicitly[Sync[IO]], decoder)
  }
}

object IOProjectPathFinder {

  def apply(
      logger: Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[ProjectPathFinder[IO]] =
    for {
      gitLabRateLimit <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit")
      gitLabThrottler <- Throttler[IO, GitLab](gitLabRateLimit)
      gitLabUrl       <- GitLabUrl[IO]()
    } yield new IOProjectPathFinder(gitLabUrl, gitLabThrottler, logger)
}
