/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.api

import EventLogClient.{EventPayload, Result, SearchCriteria}
import Http4sEventLogClient.Implicits._
import cats.effect._
import cats.syntax.all._
import io.renku.control.Throttler
import io.renku.graph.model.eventlogapi.ServiceStatus
import io.renku.graph.model.events.{EventId, EventInfo}
import io.renku.graph.model.projects.{Slug => ProjectSlug}
import io.renku.http.client.RestClient
import io.renku.tinytypes.TinyType
import org.http4s.Uri.Path.SegmentEncoder
import org.http4s.circe.CirceEntityCodec._
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.http4s.{QueryParamEncoder, Uri}
import org.typelevel.log4cats.Logger
import scodec.bits.ByteVector

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

private[eventlog] final class Http4sEventLogClient[F[_]: Async: Logger](baseUri: Uri)
    extends RestClient[F, Nothing](Throttler.noThrottling)
    with EventLogClient[F]
    with Http4sDsl[F]
    with Http4sClientDsl[F] {

  def getEvents(criteria: EventLogClient.SearchCriteria): F[Result[List[EventInfo]]] = {
    val request = GET(
      (baseUri / "events")
        .withOptionQueryParam("status", criteria.status)
        .withOptionQueryParam("since", criteria.since)
        .withOptionQueryParam("until", criteria.until)
        .withOptionQueryParam("project-id", criteria.projectId)
        .withOptionQueryParam("project-slug", criteria.projectSlug)
        .withQueryParam("page", criteria.page)
        .withOptionQueryParam("per_page", criteria.perPage)
        .withOptionQueryParam("sort", criteria.sort)
    )

    send(request) {
      case (Ok, _, resp) => resp.as[List[EventInfo]].map(Result.success)
      case (ServiceUnavailable, _, _) =>
        Logger[F]
          .info("The event-log service is currently not available")
          .as(Result.unavailable)
      case (status, req, _) =>
        Logger[F]
          .error(show"Unexpected response from event-log to request ${req.pathInfo.renderString}: $status")
          .as(Result.failure(show"Unexpected response: $status"))
    }
  }

  def getEventPayload(eventId: EventId, projectSlug: ProjectSlug): F[Result[Option[EventPayload]]] = {
    val request = GET(
      baseUri / "events" / eventId / projectSlug / "payload"
    )

    send(request) {
      case (Ok, _, resp) =>
        resp.as[ByteVector].map(bv => Result.success(EventPayload(bv).some))
      case (NotFound, _, _) =>
        Result.success(Option.empty[EventPayload]).pure[F]
      case (ServiceUnavailable, _, _) =>
        Logger[F]
          .info("The event-log service is currently not available")
          .as(Result.unavailable)
      case (status, req, _) =>
        Logger[F]
          .error(show"Unexpected response from event-log to request ${req.pathInfo.renderString}: $status")
          .as(Result.failure(show"Unexpected response: $status"))
    }
  }

  override def getStatus: F[Result[ServiceStatus]] =
    send(GET(baseUri / "status")) {
      case (Ok, _, resp) =>
        resp.as[ServiceStatus].map(Result.success)
      case (ServiceUnavailable, _, _) =>
        Logger[F]
          .info("The event-log service is currently not available")
          .as(Result.unavailable)
      case (status, req, _) =>
        Logger[F]
          .error(show"Unexpected response from event-log to request ${req.pathInfo.renderString}: $status")
          .as(Result.failure(show"Unexpected response: $status"))
    }
}

object Http4sEventLogClient {

  object Implicits {
    implicit def tinyTypeQueryParamEncoder[A <: TinyType](implicit qe: QueryParamEncoder[A#V]): QueryParamEncoder[A] =
      qe.contramap(_.value)

    implicit def tinyTypeSegmentEncoder[A <: TinyType](implicit se: SegmentEncoder[A#V]): SegmentEncoder[A] =
      se.contramap(_.value)

    implicit val sortQueryParamEncoder: QueryParamEncoder[SearchCriteria.Sort] =
      QueryParamEncoder.stringQueryParamEncoder.contramap {
        case SearchCriteria.Sort.EventDateAsc  => "eventDate:ASC"
        case SearchCriteria.Sort.EventDateDesc => "eventDate:DESC"
      }

    implicit val instantParamEncoder: QueryParamEncoder[Instant] =
      QueryParamEncoder.instant(DateTimeFormatter.ISO_INSTANT).contramap(_.truncatedTo(ChronoUnit.SECONDS))
  }
}
