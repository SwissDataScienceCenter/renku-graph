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

package io.renku.commiteventservice.events.categories.commitsync.eventgeneration

import cats.effect.{Async, Temporal}
import cats.syntax.all._
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.circe.parser.parse
import io.renku.commiteventservice.events.categories.common.CommitWithParents
import io.renku.control.Throttler
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import io.renku.http.client.RestClient
import org.http4s.Status.{NotFound, Ok}
import org.http4s._
import org.http4s.circe.jsonOf
import org.typelevel.log4cats.Logger

private trait EventDetailsFinder[F[_]] {
  def getEventDetails(projectId: projects.Id, commitId: CommitId): F[Option[CommitWithParents]]
}

private class EventDetailsFinderImpl[F[_]: Async: Temporal: Logger](
    eventLogUrl: EventLogUrl
) extends RestClient[F, EventDetailsFinder[F]](Throttler.noThrottling)
    with EventDetailsFinder[F] {

  import org.http4s.Method.GET

  override def getEventDetails(projectId: projects.Id, commitId: CommitId): F[Option[CommitWithParents]] =
    validateUri(s"$eventLogUrl/events/$commitId/$projectId") >>=
      (uri => send(request(GET, uri))(mapResponseCommitDetails))

  private lazy val mapResponseCommitDetails
      : PartialFunction[(Status, Request[F], Response[F]), F[Option[CommitWithParents]]] = {
    case (Ok, _, response) => response.as[CommitWithParents].map(_.some)
    case (NotFound, _, _)  => Option.empty[CommitWithParents].pure[F]
  }

  import io.renku.tinytypes.json.TinyTypeDecoders._
  private implicit val commitDetailsEntityDecoder: EntityDecoder[F, CommitWithParents] = {
    implicit val commitDecoder: Decoder[CommitWithParents] = cursor =>
      for {
        id         <- cursor.downField("id").as[CommitId]
        projectId  <- cursor.downField("project").downField("id").as[projects.Id]
        bodyString <- cursor.downField("body").as[String]
        parents = parse(bodyString)
                    .map(eventBodyJson =>
                      eventBodyJson.hcursor.downField("parents").as[List[CommitId]].getOrElse(List.empty[CommitId])
                    )
                    .getOrElse(List.empty[CommitId])
      } yield CommitWithParents(id, projectId, parents)
    jsonOf[F, CommitWithParents]
  }
}

private object EventDetailsFinder {
  def apply[F[_]: Async: Temporal: Logger]: F[EventDetailsFinderImpl[F]] = for {
    eventLogUrl <- EventLogUrl[F]()
  } yield new EventDetailsFinderImpl(eventLogUrl)
}
