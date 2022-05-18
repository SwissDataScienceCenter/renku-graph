/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.rest

import cats.effect.kernel.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.{Id, Visibility}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.knowledgegraph.projects.model.Forking.ForksCount
import io.renku.knowledgegraph.projects.model.Project.{DateUpdated, StarsCount}
import io.renku.knowledgegraph.projects.model._
import io.renku.knowledgegraph.projects.rest.GitLabProjectFinder.GitLabProject
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.log4cats.Logger

private trait GitLabProjectFinder[F[_]] {
  def findProject(projectPath: projects.Path)(implicit accessToken: AccessToken): F[Option[GitLabProject]]
}

private class GitLabProjectFinderImpl[F[_]: Async: Logger](
    gitLabClient: GitLabClient[F]
) extends GitLabProjectFinder[F] {

  import cats.syntax.all._
  import io.circe._
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import org.http4s._
  import org.http4s.circe.jsonOf
  import org.http4s.dsl.io._

  def findProject(projectPath: projects.Path)(implicit accessToken: AccessToken): F[Option[GitLabProject]] =
    gitLabClient.get(uri"projects" / projectPath.value withQueryParam ("statistics", "true"), "single-project")(
      mapResponse
    )(accessToken.some)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[GitLabProject]]] = {
    case (Ok, _, response) => response.as[GitLabProject].map(Option.apply)
    case (NotFound, _, _)  => Option.empty[GitLabProject].pure[F]
  }

  private implicit lazy val projectDecoder: EntityDecoder[F, GitLabProject] = {
    import io.renku.knowledgegraph.projects.model.Forking.ForksCount
    import io.renku.knowledgegraph.projects.model.Permissions._
    import io.renku.knowledgegraph.projects.model.Project.StarsCount
    import io.renku.knowledgegraph.projects.model.Statistics._
    import io.renku.knowledgegraph.projects.model.Urls
    import io.renku.knowledgegraph.projects.model.Urls._

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
                         case _ => Left(DecodingFailure("permissions has neither project_access nor group_access", Nil))
                       }
      } yield permissions
    }

    implicit val decoder: Decoder[GitLabProject] = cursor =>
      for {
        id             <- cursor.downField("id").as[Id]
        sshUrl         <- cursor.downField("ssh_url_to_repo").as[SshUrl]
        httpUrl        <- cursor.downField("http_url_to_repo").as[HttpUrl]
        webUrl         <- cursor.downField("web_url").as[WebUrl]
        maybeReadmeUrl <- cursor.downField("readme_url").as[Option[ReadmeUrl]]
        forksCount     <- cursor.downField("forks_count").as[ForksCount]
        visibility     <- cursor.downField("visibility").as[Visibility]
        starsCount     <- cursor.downField("star_count").as[StarsCount]
        updatedAt      <- cursor.downField("last_activity_at").as[DateUpdated]
        statistics     <- cursor.downField("statistics").as[Statistics]
        permissions    <- cursor.downField("permissions").as[Permissions]
      } yield GitLabProject(
        id,
        visibility,
        Urls(sshUrl, httpUrl, webUrl, maybeReadmeUrl),
        forksCount,
        starsCount,
        updatedAt,
        permissions,
        statistics
      )

    jsonOf[F, GitLabProject]
  }
}

private object GitLabProjectFinder {

  final case class GitLabProject(id:          Id,
                                 visibility:  Visibility,
                                 urls:        Urls,
                                 forksCount:  ForksCount,
                                 starsCount:  StarsCount,
                                 updatedAt:   DateUpdated,
                                 permissions: Permissions,
                                 statistics:  Statistics
  )

  def apply[F[_]: Async: Logger](gitLabClient: GitLabClient[F]): F[GitLabProjectFinder[F]] =
    new GitLabProjectFinderImpl(gitLabClient).pure[F].widen
}
