/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.webhookservice.eventstatus

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.eventlog.EventLogClient
import io.renku.graph.model.events.EventInfo
import io.renku.graph.model.projects
import io.renku.http.rest.paging.model.PerPage
import org.typelevel.log4cats.Logger

private trait StatusInfoFinder[F[_]] {
  def findStatusInfo(projectId: projects.GitLabId): F[Option[StatusInfo]]
}

private object StatusInfoFinder {

  def apply[F[_]: Async: Logger]: F[StatusInfoFinder[F]] =
    EventLogClient[F].map(new StatusInfoFinderImpl(_))
}

private class StatusInfoFinderImpl[F[_]: MonadThrow](eventLogClient: EventLogClient[F]) extends StatusInfoFinder[F] {

  override def findStatusInfo(projectId: projects.GitLabId): F[Option[StatusInfo]] =
    eventLogClient
      .getEvents(
        EventLogClient.SearchCriteria
          .forProject(projectId)
          .withPerPage(PerPage(1))
          .sortBy(EventLogClient.SearchCriteria.Sort.EventDateDesc)
      )
      .map(_.toEither.map(toStatusInfo))
      .rethrow

  private def toStatusInfo(events: List[EventInfo]): Option[StatusInfo] =
    events match {
      case Nil        => None
      case event :: _ => StatusInfo.activated(event).some
    }
}
