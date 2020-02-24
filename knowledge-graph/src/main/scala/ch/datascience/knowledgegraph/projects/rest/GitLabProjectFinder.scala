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

package ch.datascience.knowledgegraph.projects.rest

import cats.data.OptionT
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.projects.{ProjectId, ProjectPath, ProjectVisibility}
import ch.datascience.http.client.{AccessToken, IORestClient}
import ch.datascience.knowledgegraph.config.GitLab
import ch.datascience.knowledgegraph.projects.model.RepoUrls.{HttpUrl, SshUrl}
import ch.datascience.knowledgegraph.projects.rest.GitLabProjectFinder.{GitLabProject, ProjectUrls}
import io.chrisdavenport.log4cats.Logger
import org.http4s.circe.jsonOf

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait GitLabProjectFinder[Interpretation[_]] {
  def findProject(
      projectPath:      ProjectPath,
      maybeAccessToken: Option[AccessToken]
  ): OptionT[Interpretation, GitLabProject]
}

object GitLabProjectFinder {

  final case class GitLabProject(id: ProjectId, visibility: ProjectVisibility, urls: ProjectUrls)

  final case class ProjectUrls(http: HttpUrl, ssh: SshUrl)
}

private class IOGitLabProjectFinder(
    gitLabUrl:               GitLabUrl,
    gitLabThrottler:         Throttler[IO, GitLab],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient(gitLabThrottler, logger)
    with GitLabProjectFinder[IO] {

  import cats.effect._
  import cats.implicits._
  import ch.datascience.http.client.UrlEncoder.urlEncode
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe._
  import org.http4s.Method.GET
  import org.http4s._
  import org.http4s.dsl.io._

  def findProject(projectPath: ProjectPath, maybeAccessToken: Option[AccessToken]): OptionT[IO, GitLabProject] =
    OptionT {
      for {
        uri     <- validateUri(s"$gitLabUrl/api/v4/projects/${urlEncode(projectPath.value)}")
        project <- send(request(GET, uri, maybeAccessToken))(mapResponse)
      } yield project
    }

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Option[GitLabProject]]] = {
    case (Ok, _, response) => response.as[GitLabProject].map(Option.apply)
    case (NotFound, _, _)  => None.pure[IO]
  }

  private implicit lazy val projectDecoder: EntityDecoder[IO, GitLabProject] = {
    implicit val decoder: Decoder[GitLabProject] = cursor =>
      for {
        id         <- cursor.downField("id").as[ProjectId]
        visibility <- cursor.downField("visibility").as[ProjectVisibility]
        sshUrl     <- cursor.downField("ssh_url_to_repo").as[SshUrl]
        httpUrl    <- cursor.downField("http_url_to_repo").as[HttpUrl]
      } yield GitLabProject(id, visibility, ProjectUrls(httpUrl, sshUrl))

    jsonOf[IO, GitLabProject]
  }
}

object IOGitLabProjectFinder {

  def apply(
      gitLabThrottler:         Throttler[IO, GitLab],
      logger:                  Logger[IO]
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[GitLabProjectFinder[IO]] =
    for {
      gitLabUrl <- GitLabUrl[IO]()
    } yield new IOGitLabProjectFinder(gitLabUrl, gitLabThrottler, logger)
}
