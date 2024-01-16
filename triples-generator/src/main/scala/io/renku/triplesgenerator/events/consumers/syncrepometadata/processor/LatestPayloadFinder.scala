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

package io.renku.triplesgenerator.events.consumers.syncrepometadata.processor

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.eventlog.api.EventLogClient
import io.renku.eventlog.api.EventLogClient.{EventPayload, SearchCriteria}
import io.renku.graph.model.events.{EventInfo, EventStatus}
import io.renku.graph.model.{events, projects}
import io.renku.http.rest.paging.model.PerPage
import org.typelevel.log4cats.Logger

private trait LatestPayloadFinder[F[_]] {
  def fetchLatestPayload(slug: projects.Slug): F[Option[EventPayload]]
}

private object LatestPayloadFinder {
  def apply[F[_]: Async: Logger]: F[LatestPayloadFinder[F]] =
    EventLogClient[F].map(new LatestPayloadFinderImpl[F](_))
}

private class LatestPayloadFinderImpl[F[_]: MonadThrow](elClient: EventLogClient[F]) extends LatestPayloadFinder[F] {

  override def fetchLatestPayload(slug: projects.Slug): F[Option[EventPayload]] =
    findMostRecentSuccessfulEventId(slug) >>= {
      case None          => Option.empty[EventPayload].pure[F]
      case Some(eventId) => findEventPayload(eventId, slug)
    }

  private def findMostRecentSuccessfulEventId(slug: projects.Slug) =
    elClient
      .getEvents(
        SearchCriteria
          .forProject(slug)
          .withStatus(EventStatus.TriplesStore)
          .withPerPage(PerPage(1))
          .sortBy(SearchCriteria.Sort.EventDateDesc)
      )
      .flatMap(raiseErrorWhenException[List[EventInfo]])
      .map(_.headOption)
      .map(_.map(_.eventId))

  private def findEventPayload(eventId: events.EventId, slug: projects.Slug) =
    elClient
      .getEventPayload(eventId, slug)
      .flatMap(raiseErrorWhenException[Option[EventPayload]])

  private def raiseErrorWhenException[O](result: EventLogClient.Result[O]) =
    result.toEither.fold(t => t.raiseError[F, O], _.pure[F])
}
