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

package ch.datascience.webhookservice.pushevents

import LatestPushEventFetcher.PushEventInfo
import cats.effect.{ContextShift, IO}
import ch.datascience.graph.model.events._
import ch.datascience.http.client.{AccessToken, IORestClient}
import ch.datascience.webhookservice.config.GitLabConfigProvider
import org.http4s.Status

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait LatestPushEventFetcher[Interpretation[_]] {
  def fetchLatestPushEvent(
      projectId:        ProjectId,
      maybeAccessToken: Option[AccessToken]
  ): Interpretation[Option[PushEventInfo]]
}

object LatestPushEventFetcher {

  final case class PushEventInfo(
      projectId: ProjectId,
      pushUser:  PushUser,
      commitTo:  CommitId
  )
}

class IOLatestPushEventFetcher(
    gitLabConfig:            GitLabConfigProvider[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO])
    extends IORestClient
    with LatestPushEventFetcher[IO] {

  import cats.implicits._
  import ch.datascience.http.client.RestClientError.UnauthorizedException
  import io.circe.{Decoder, HCursor}
  import org.http4s.Method.GET
  import org.http4s.Status._
  import org.http4s.circe.jsonOf
  import org.http4s.{EntityDecoder, Request, Response}

  override def fetchLatestPushEvent(
      projectId:        ProjectId,
      maybeAccessToken: Option[AccessToken]
  ): IO[Option[PushEventInfo]] =
    for {
      gitLabHostUrl <- gitLabConfig.get
      uri           <- validateUri(s"$gitLabHostUrl/api/v4/projects/$projectId/events") map (_.withQueryParam("action", "pushed"))
      projectInfo   <- send(request(GET, uri, maybeAccessToken))(mapResponse)
    } yield projectInfo

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Option[PushEventInfo]]] = {
    case (Ok, _, response)    => response.as[Option[PushEventInfo]]
    case (NotFound, _, _)     => IO.pure(None)
    case (Unauthorized, _, _) => IO.raiseError(UnauthorizedException)
  }

  private implicit lazy val pushEventInfoDecoder: EntityDecoder[IO, Option[PushEventInfo]] = {
    implicit val hookNameDecoder: Decoder[Option[PushEventInfo]] = (cursor: HCursor) =>
      for {
        maybeProjectId      <- cursor.downArray.downField("project_id").as[Option[ProjectId]]
        maybeAuthorId       <- cursor.downArray.downField("author_id").as[Option[UserId]]
        maybeAuthorUsername <- cursor.downArray.downField("author_username").as[Option[Username]]
        maybeCommitTo       <- cursor.downArray.downField("push_data").downField("commit_to").as[Option[CommitId]]
      } yield
        (maybeProjectId, maybeAuthorId, maybeAuthorUsername, maybeCommitTo) mapN {
          case (projectId, authorId, authorUsername, commitTo) =>
            PushEventInfo(projectId, PushUser(authorId, authorUsername, maybeEmail = None), commitTo)
      }

    jsonOf[IO, Option[PushEventInfo]]
  }
}
