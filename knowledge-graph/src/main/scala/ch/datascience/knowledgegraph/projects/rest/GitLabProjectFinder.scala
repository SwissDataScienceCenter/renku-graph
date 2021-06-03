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

package ch.datascience.knowledgegraph.projects.rest

import cats.data.OptionT
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{Description, Id, Visibility}
import ch.datascience.http.client.{AccessToken, RestClient}
import ch.datascience.knowledgegraph.projects.model.Forking.ForksCount
import ch.datascience.knowledgegraph.projects.model.Project.{DateUpdated, StarsCount, Tag}
import ch.datascience.knowledgegraph.projects.model._
import ch.datascience.knowledgegraph.projects.rest.GitLabProjectFinder.GitLabProject
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

trait GitLabProjectFinder[Interpretation[_]] {
  def findProject(
      projectPath:      projects.Path,
      maybeAccessToken: Option[AccessToken]
  ): OptionT[Interpretation, GitLabProject]
}

object GitLabProjectFinder {

  final case class GitLabProject(id:               Id,
                                 maybeDescription: Option[Description],
                                 visibility:       Visibility,
                                 urls:             Urls,
                                 forksCount:       ForksCount,
                                 tags:             Set[Tag],
                                 starsCount:       StarsCount,
                                 updatedAt:        DateUpdated,
                                 permissions:      Permissions,
                                 statistics:       Statistics
  )
}

private class IOGitLabProjectFinder(
    gitLabUrl:               GitLabUrl,
    gitLabThrottler:         Throttler[IO, GitLab],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends RestClient(gitLabThrottler, logger)
    with GitLabProjectFinder[IO] {

  import cats.effect._
  import cats.syntax.all._
  import ch.datascience.http.client.UrlEncoder.urlEncode
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe._
  import org.http4s.Method.GET
  import org.http4s._
  import org.http4s.circe.jsonOf
  import org.http4s.dsl.io._

  def findProject(projectPath: projects.Path, maybeAccessToken: Option[AccessToken]): OptionT[IO, GitLabProject] =
    OptionT {
      for {
        uri     <- validateUri(s"$gitLabUrl/api/v4/projects/${urlEncode(projectPath.value)}?statistics=true")
        project <- send(request(GET, uri, maybeAccessToken))(mapResponse)
      } yield project
    }

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Option[GitLabProject]]] = {
    case (Ok, _, response) => response.as[GitLabProject].map(Option.apply)
    case (NotFound, _, _)  => None.pure[IO]
  }

  private implicit lazy val projectDecoder: EntityDecoder[IO, GitLabProject] = {
    import ch.datascience.knowledgegraph.projects.model.Forking.ForksCount
    import ch.datascience.knowledgegraph.projects.model.Permissions._
    import ch.datascience.knowledgegraph.projects.model.Project.StarsCount
    import ch.datascience.knowledgegraph.projects.model.Statistics._
    import ch.datascience.knowledgegraph.projects.model.Urls
    import ch.datascience.knowledgegraph.projects.model.Urls._

    implicit val maybeAccessLevelDecoder: Decoder[Option[AccessLevel]] =
      _.as[Option[Json]].flatMap {
        case None => Right(Option.empty[AccessLevel])
        case Some(json) =>
          json.hcursor
            .downField("access_level")
            .as[Option[Int]]
            .flatMap {
              case Some(level) => (AccessLevel from level) map Option.apply
              case None        => Right(Option.empty[AccessLevel])
            }
            .leftMap(exception => DecodingFailure(exception.getMessage, Nil))
      }

    implicit val statisticsDecoder: Decoder[Statistics] = cursor =>
      for {
        commitsCount     <- cursor.downField("commit_count").as[CommitsCount]
        storageSize      <- cursor.downField("storage_size").as[StorageSize]
        repositorySize   <- cursor.downField("repository_size").as[RepositorySize]
        lfsSize          <- cursor.downField("lfs_objects_size").as[LsfObjectsSize]
        jobArtifactsSize <- cursor.downField("job_artifacts_size").as[JobArtifactsSize]
      } yield Statistics(commitsCount, storageSize, repositorySize, lfsSize, jobArtifactsSize)

    implicit val permissionsDecoder: Decoder[Permissions] = cursor => {

      def maybeAccessLevel(name: String) = cursor.downField(name).as[Option[AccessLevel]]

      for {
        maybeProjectAccessLevel <- maybeAccessLevel("project_access").map(_.map(ProjectAccessLevel))
        maybeGroupAccessLevel   <- maybeAccessLevel("group_access").map(_.map(GroupAccessLevel))
        permissions <- (maybeProjectAccessLevel, maybeGroupAccessLevel) match {
                         case (Some(project), Some(group)) => Right(Permissions(project, group))
                         case (Some(project), None)        => Right(Permissions(project))
                         case (None, Some(group))          => Right(Permissions(group))
                         case _                            => Left(DecodingFailure("permissions has neither project_access nor group_access", Nil))
                       }
      } yield permissions
    }

    implicit val decoder: Decoder[GitLabProject] = cursor =>
      for {
        id             <- cursor.downField("id").as[Id]
        visibility     <- cursor.downField("visibility").as[Visibility]
        sshUrl         <- cursor.downField("ssh_url_to_repo").as[SshUrl]
        httpUrl        <- cursor.downField("http_url_to_repo").as[HttpUrl]
        webUrl         <- cursor.downField("web_url").as[WebUrl]
        maybeReadmeUrl <- cursor.downField("readme_url").as[Option[ReadmeUrl]]
        forksCount     <- cursor.downField("forks_count").as[ForksCount]
        tags           <- cursor.downField("tag_list").as[List[Tag]]
        starsCount     <- cursor.downField("star_count").as[StarsCount]
        updatedAt      <- cursor.downField("last_activity_at").as[DateUpdated]
        statistics     <- cursor.downField("statistics").as[Statistics]
        permissions    <- cursor.downField("permissions").as[Permissions]
        maybeDescription <- cursor
                              .downField("description")
                              .as[Option[String]]
                              .map(blankToNone)
                              .flatMap(toOption[Description])
      } yield GitLabProject(
        id,
        maybeDescription,
        visibility,
        Urls(sshUrl, httpUrl, webUrl, maybeReadmeUrl),
        forksCount,
        tags.toSet,
        starsCount,
        updatedAt,
        permissions,
        statistics
      )

    jsonOf[IO, GitLabProject]
  }
}

object IOGitLabProjectFinder {

  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[GitLabProjectFinder[IO]] =
    for {
      gitLabUrl <- GitLabUrl[IO]()
    } yield new IOGitLabProjectFinder(gitLabUrl, gitLabThrottler, logger)
}
