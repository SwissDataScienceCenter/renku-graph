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

package io.renku.commiteventservice.events.categories.common

import cats.effect.{Async, Temporal}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.events._
import io.renku.graph.model.projects.Id
import io.renku.http.client.{AccessToken, GitLabClient}
import org.http4s.Status.NotFound
import org.http4s.circe.jsonOf
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{EntityDecoder, Status}
import org.typelevel.log4cats.Logger

private[categories] trait CommitInfoFinder[F[_]] {
  def findCommitInfo(
      projectId: Id,
      commitId:  CommitId
  )(implicit
      maybeAccessToken: Option[AccessToken]
  ): F[CommitInfo]

  def getMaybeCommitInfo(projectId: Id, commitId: CommitId)(implicit
      maybeAccessToken:             Option[AccessToken]
  ): F[Option[CommitInfo]]
}

private[categories] class CommitInfoFinderImpl[F[_]: Async: Temporal: Logger](gitLabClient: GitLabClient[F])
    extends CommitInfoFinder[F] {

  import CommitInfo._
  import org.http4s.Method.GET
  import org.http4s.Status.{Ok, Unauthorized}
  import org.http4s.{Request, Response}

  def findCommitInfo(projectId: Id, commitId: CommitId)(implicit
      maybeAccessToken:         Option[AccessToken]
  ): F[CommitInfo] =
    fetchCommitInfo(projectId, commitId)(mapToCommitOrThrow(projectId, commitId))

  def getMaybeCommitInfo(projectId: Id, commitId: CommitId)(implicit
      maybeAccessToken:             Option[AccessToken]
  ): F[Option[CommitInfo]] =
    fetchCommitInfo(projectId, commitId)(mapToMaybeCommit(projectId, commitId))

  private def fetchCommitInfo[ResultType](projectId: Id, commitId: CommitId)(
      mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[
        ResultType
      ]]
  )(implicit maybeAccessToken: Option[AccessToken]) =
    gitLabClient.send(GET, uri"projects" / projectId.show / "repository" / "commits" / commitId.show, "commit-details")(
      mapResponse
    )

  private def mapToCommitOrThrow(projectId: Id,
                                 commitId:  CommitId
  ): PartialFunction[(Status, Request[F], Response[F]), F[CommitInfo]] = {
    case (Ok, _, response)    => response.as[CommitInfo]
    case (Unauthorized, _, _) => findCommitInfo(projectId, commitId)(maybeAccessToken = None)
  }

  private def mapToMaybeCommit(projectId: Id,
                               commitId:  CommitId
  ): PartialFunction[(Status, Request[F], Response[F]), F[Option[CommitInfo]]] = {
    case (Ok, _, response)    => response.as[CommitInfo].map(Some(_))
    case (NotFound, _, _)     => Option.empty[CommitInfo].pure[F]
    case (Unauthorized, _, _) => getMaybeCommitInfo(projectId, commitId)(maybeAccessToken = None)
  }

  private implicit val commitInfoEntityDecoder: EntityDecoder[F, CommitInfo] = jsonOf[F, CommitInfo]
}

private[categories] object CommitInfoFinder {
  def apply[F[_]: Async: Temporal: Logger](gitLabClient: GitLabClient[F]): F[CommitInfoFinderImpl[F]] =
    new CommitInfoFinderImpl[F](gitLabClient).pure[F]
}
