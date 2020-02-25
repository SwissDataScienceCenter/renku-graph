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
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{Description, Id, Visibility}
import ch.datascience.http.client.{AccessToken, IORestClient}
import ch.datascience.knowledgegraph.config.GitLab
import ch.datascience.knowledgegraph.projects.model.RepoUrls.{HttpUrl, SshUrl}
import ch.datascience.knowledgegraph.projects.rest.GitLabProjectFinder.{DateUpdated, GitLabProject}
import io.chrisdavenport.log4cats.Logger
import org.http4s.circe.jsonOf

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait GitLabProjectFinder[Interpretation[_]] {
  def findProject(
      projectPath:      projects.Path,
      maybeAccessToken: Option[AccessToken]
  ): OptionT[Interpretation, GitLabProject]
}

object GitLabProjectFinder {
  import java.time.Instant

  import ch.datascience.tinytypes.constraints.{InstantNotInTheFuture, NonNegativeInt}
  import ch.datascience.tinytypes.{InstantTinyType, IntTinyType, TinyTypeFactory}

  final case class GitLabProject(id:               Id,
                                 maybeDescription: Option[Description],
                                 visibility:       Visibility,
                                 urls:             ProjectUrls,
                                 forksCount:       ForksCount,
                                 starsCount:       StarsCount,
                                 updatedAt:        DateUpdated)

  final case class ProjectUrls(http: HttpUrl, ssh: SshUrl)

  final class ForksCount private (val value: Int) extends AnyVal with IntTinyType
  implicit object ForksCount extends TinyTypeFactory[ForksCount](new ForksCount(_)) with NonNegativeInt

  final class StarsCount private (val value: Int) extends AnyVal with IntTinyType
  implicit object StarsCount extends TinyTypeFactory[StarsCount](new StarsCount(_)) with NonNegativeInt

  final class DateUpdated private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object DateUpdated extends TinyTypeFactory[DateUpdated](new DateUpdated(_)) with InstantNotInTheFuture
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

  def findProject(projectPath: projects.Path, maybeAccessToken: Option[AccessToken]): OptionT[IO, GitLabProject] =
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
    import ch.datascience.knowledgegraph.projects.model.RepoUrls.{HttpUrl, SshUrl}
    import ch.datascience.knowledgegraph.projects.rest.GitLabProjectFinder.{ForksCount, GitLabProject, ProjectUrls, StarsCount}

    implicit val decoder: Decoder[GitLabProject] = cursor =>
      for {
        id <- cursor.downField("id").as[Id]
        maybeDescription <- cursor
                             .downField("description")
                             .as[Option[String]]
                             .map(blankToNone)
                             .flatMap(toOption[Description])
        visibility <- cursor.downField("visibility").as[Visibility]
        sshUrl     <- cursor.downField("ssh_url_to_repo").as[SshUrl]
        httpUrl    <- cursor.downField("http_url_to_repo").as[HttpUrl]
        forksCount <- cursor.downField("forks_count").as[ForksCount]
        starsCount <- cursor.downField("star_count").as[StarsCount]
        updatedAt  <- cursor.downField("last_activity_at").as[DateUpdated]
      } yield GitLabProject(id,
                            maybeDescription,
                            visibility,
                            ProjectUrls(httpUrl, sshUrl),
                            forksCount,
                            starsCount,
                            updatedAt)

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
