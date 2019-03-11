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

package ch.datascience.webhookservice.hookcreation

import cats.effect.{ContextShift, IO}
import ch.datascience.graph.model.events.{CommitId, ProjectId, UserId}
import ch.datascience.http.client.{AccessToken, IORestClient}
import ch.datascience.webhookservice.config.GitLabConfigProvider
import ch.datascience.webhookservice.eventprocessing.pushevent.CommitInfo
import ch.datascience.webhookservice.hookcreation.LatestPushEventFetcher.PushEventInfo
import org.http4s.Status

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait LatestPushEventFetcher[Interpretation[_]] {
  def fetchLatestPushEvent(
      projectId:   ProjectId,
      accessToken: AccessToken
  ): Interpretation[Option[PushEventInfo]]
}

private object LatestPushEventFetcher {

  final case class PushEventInfo(
      projectId: ProjectId,
      authorId:  UserId,
      commitTo:  CommitId
  )
}

private class IOLatestPushEventFetcher(
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
      projectId:   ProjectId,
      accessToken: AccessToken
  ): IO[Option[PushEventInfo]] =
    for {
      gitLabHostUrl <- gitLabConfig.get
      uri           <- validateUri(s"$gitLabHostUrl/api/v4/projects/$projectId/events") map (_.withQueryParam("action", "pushed"))
      projectInfo   <- send(request(GET, uri, accessToken))(mapResponse)
    } yield projectInfo

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Option[PushEventInfo]]] = {
    case (Ok, _, response)    => response.as[Option[PushEventInfo]]
    case (NotFound, _, _)     => IO.pure(None)
    case (Unauthorized, _, _) => IO.raiseError(UnauthorizedException)
  }

  private implicit lazy val pushEventInfoDecoder: EntityDecoder[IO, Option[PushEventInfo]] = {
    implicit val hookNameDecoder: Decoder[Option[PushEventInfo]] = (cursor: HCursor) =>
      for {
        maybeProjectId <- cursor.downArray.downField("project_id").as[Option[ProjectId]]
        maybeAuthorId  <- cursor.downArray.downField("author_id").as[Option[UserId]]
        maybeCommitTo  <- cursor.downArray.downField("push_data").downField("commit_to").as[Option[CommitId]]
      } yield
        (maybeProjectId, maybeAuthorId, maybeCommitTo) mapN {
          case (projectId, authorId, commitTo) => PushEventInfo(projectId, authorId, commitTo)
      }

    jsonOf[IO, Option[PushEventInfo]]
  }
}
