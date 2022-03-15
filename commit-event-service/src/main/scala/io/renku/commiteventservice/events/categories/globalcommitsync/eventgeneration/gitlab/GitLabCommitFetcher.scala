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

package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration
package gitlab

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.commiteventservice.events.categories.common.CommitInfo
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.model.Page
import org.http4s.Method.GET
import org.http4s.Status.{Forbidden, NotFound, Ok, Unauthorized}
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

private[globalcommitsync] trait GitLabCommitFetcher[F[_]] {

  def fetchLatestGitLabCommit(projectId: projects.Id)(implicit
      maybeAccessToken:                  Option[AccessToken]
  ): F[Option[CommitId]]

  def fetchGitLabCommits(projectId: projects.Id, dateCondition: DateCondition, pageRequest: PagingRequest)(implicit
      maybeAccessToken:             Option[AccessToken]
  ): F[PageResult]
}

private[globalcommitsync] class GitLabCommitFetcherImpl[F[_]: Async](
    gitLabClientImpl: GitLabClient[F]
) extends GitLabCommitFetcher[F] {

  import gitLabClientImpl._

  override def fetchLatestGitLabCommit(projectId: projects.Id)(implicit
      maybeAccessToken:                           Option[AccessToken]
  ): F[Option[CommitId]] = send(
    GET,
    uri"projects" / projectId.show / "repository" / "commits" withQueryParam ("per_page", "1"),
    "latest commit"
  )(mapLatestCommit(projectId))

  override def fetchGitLabCommits(
      projectId:               projects.Id,
      dateCondition:           DateCondition,
      pageRequest:             PagingRequest
  )(implicit maybeAccessToken: Option[AccessToken]): F[PageResult] = send(
    GET,
    uri"projects" / projectId.show / "repository" / "commits" withQueryParams Map(
      "page"     -> pageRequest.page.show,
      "per_page" -> pageRequest.perPage.show
    ) + dateCondition.asQueryParameter,
    "commits"
  )(mapCommitsPage(projectId, dateCondition, pageRequest))

  private def mapCommitsPage(projectId:     projects.Id,
                             dateCondition: DateCondition,
                             pageRequest:   PagingRequest
  ): PartialFunction[(Status, Request[F], Response[F]), F[PageResult]] = {
    case (Ok, _, response) => (response.as[List[CommitId]] -> maybeNextPage(response)).mapN(PageResult(_, _))
    case (NotFound, _, _)  => PageResult.empty.pure[F]
    case (Unauthorized | Forbidden, _, _) =>
      fetchGitLabCommits(projectId, dateCondition, pageRequest)(maybeAccessToken = None)
  }

  private def mapLatestCommit(
      projectId: projects.Id
  ): PartialFunction[(Status, Request[F], Response[F]), F[Option[CommitId]]] = {
    case (Ok, _, response)                => response.as[List[CommitId]].map(_.headOption)
    case (NotFound, _, _)                 => Option.empty[CommitId].pure[F]
    case (Unauthorized | Forbidden, _, _) => fetchLatestGitLabCommit(projectId)(maybeAccessToken = None)
  }

  private implicit val commitIdDecoder: EntityDecoder[F, List[CommitId]] =
    jsonOf[F, List[CommitInfo]].map(_.map(_.id))

  private def maybeNextPage(response: Response[F]): F[Option[Page]] =
    response.headers
      .get(ci"X-Next-Page")
      .flatMap(_.head.value.toIntOption)
      .map(Page.from)
      .map(MonadThrow[F].fromEither(_))
      .sequence
}

private[globalcommitsync] object GitLabCommitFetcher {
  def apply[F[_]: Async: Logger](gitLabClient: GitLabClient[F]): F[GitLabCommitFetcher[F]] =
    new GitLabCommitFetcherImpl[F](gitLabClient).pure[F].widen
}
