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

package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.gitlab

import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.commiteventservice.events.categories.globalcommitsync.CommitsCount
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import org.http4s.Status.{Forbidden, InternalServerError, NotFound, Ok, Unauthorized}
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.log4cats.Logger

private[globalcommitsync] trait GitLabCommitStatFetcher[F[_]] {
  def fetchCommitStats(projectId: projects.Id)(implicit
      maybeAccessToken:           Option[AccessToken]
  ): F[Option[ProjectCommitStats]]
}

private[globalcommitsync] class GitLabCommitStatFetcherImpl[F[_]: Async: GitLabClient: Logger](
    gitLabCommitFetcher: GitLabCommitFetcher[F]
) extends GitLabCommitStatFetcher[F] {

  import gitLabCommitFetcher._

  override def fetchCommitStats(
      projectId:               projects.Id
  )(implicit maybeAccessToken: Option[AccessToken]): F[Option[ProjectCommitStats]] = {
    for {
      maybeLatestCommitId <- OptionT.liftF(fetchLatestGitLabCommit(projectId))
      commitCount         <- OptionT(fetchCommitCount(projectId))
    } yield ProjectCommitStats(maybeLatestCommitId, commitCount)
  }.value

  private def fetchCommitCount(projectId: projects.Id)(implicit maybeAccessToken: Option[AccessToken]) =
    GitLabClient[F].get(uri"projects" / projectId.show withQueryParams Map("statistics" -> "true"), "single-project")(
      mapResponse
    )

  private implicit lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[CommitsCount]]] = {
    case (Ok, _, response)                                                 => response.as[Option[CommitsCount]]
    case (NotFound | InternalServerError | Unauthorized | Forbidden, _, _) => Option.empty[CommitsCount].pure[F]
  }

  private implicit val commitCountDecoder: EntityDecoder[F, Option[CommitsCount]] = {
    import io.circe.Decoder.decodeOption

    implicit val commitDecoder: Decoder[Option[CommitsCount]] =
      _.downField("statistics").downField("commit_count").as(decodeOption(CommitsCount.decoder))
    jsonOf[F, Option[CommitsCount]]
  }
}

private[globalcommitsync] object GitLabCommitStatFetcher {
  def apply[F[_]: Async: GitLabClient: Logger]: F[GitLabCommitStatFetcher[F]] = for {
    gitLabCommitFetcher <- GitLabCommitFetcher[F]
  } yield new GitLabCommitStatFetcherImpl[F](gitLabCommitFetcher)
}
