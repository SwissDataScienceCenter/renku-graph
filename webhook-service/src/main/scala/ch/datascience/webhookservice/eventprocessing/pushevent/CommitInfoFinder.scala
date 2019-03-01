/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.eventprocessing.pushevent

import cats.effect.{ContextShift, IO}
import ch.datascience.graph.model.events._
import ch.datascience.http.client.{AccessToken, IORestClient}
import ch.datascience.webhookservice.config.GitLabConfigProvider
import org.http4s.{Method, Uri}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait CommitInfoFinder[Interpretation[_]] {
  def findCommitInfo(
      projectId:        ProjectId,
      commitId:         CommitId,
      maybeAccessToken: Option[AccessToken]
  ): Interpretation[CommitInfo]
}

private class IOCommitInfoFinder(
    gitLabConfigProvider:    GitLabConfigProvider[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO])
    extends IORestClient
    with CommitInfoFinder[IO] {

  import CommitInfo._
  import cats.effect._
  import ch.datascience.webhookservice.exceptions.UnauthorizedException
  import org.http4s.Method.GET
  import org.http4s.Status.{Ok, Unauthorized}
  import org.http4s.{Request, Response}

  def findCommitInfo(projectId: ProjectId, commitId: CommitId, maybeAccessToken: Option[AccessToken]): IO[CommitInfo] =
    for {
      gitLabHost <- gitLabConfigProvider.get
      uri        <- validateUri(s"$gitLabHost/api/v4/projects/$projectId/repository/commits/$commitId")
      result     <- send(request(GET, uri, maybeAccessToken))(mapResponse)
    } yield result

  private def request(method: Method, uri: Uri, maybeAccessToken: Option[AccessToken]): Request[IO] =
    maybeAccessToken match {
      case Some(accessToken) => request(method, uri, accessToken)
      case None              => Request[IO](GET, uri)
    }

  private def mapResponse(request: Request[IO], response: Response[IO]): IO[CommitInfo] =
    response.status match {
      case Ok           => response.as[CommitInfo]
      case Unauthorized => IO.raiseError(UnauthorizedException)
      case _            => raiseError(request, response)
    }
}

private case class CommitInfo(
    id:            CommitId,
    message:       CommitMessage,
    committedDate: CommittedDate,
    author:        User,
    committer:     User,
    parents:       List[CommitId]
)

private object CommitInfo {
  import io.circe._
  import org.http4s._
  import org.http4s.circe._

  private implicit val commitInfoDecoder: Decoder[CommitInfo] = (cursor: HCursor) =>
    for {
      id             <- cursor.downField("id").as[CommitId]
      authorName     <- cursor.downField("author_name").as[Username]
      authorEmail    <- cursor.downField("author_email").as[Email]
      committerName  <- cursor.downField("committer_name").as[Username]
      committerEmail <- cursor.downField("committer_email").as[Email]
      message        <- cursor.downField("message").as[CommitMessage]
      committedDate  <- cursor.downField("committed_date").as[CommittedDate]
      parents        <- cursor.downField("parent_ids").as[List[CommitId]]
    } yield
      CommitInfo(id,
                 message,
                 committedDate,
                 author    = User(authorName, authorEmail),
                 committer = User(committerName, committerEmail),
                 parents)

  implicit val commitInfoEntityDecoder: EntityDecoder[IO, CommitInfo] = jsonOf[IO, CommitInfo]
}
