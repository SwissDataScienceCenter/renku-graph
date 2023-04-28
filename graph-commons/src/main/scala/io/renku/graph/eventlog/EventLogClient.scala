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

package io.renku.graph.eventlog

import cats.data.Ior
import cats.effect.Async
import cats.syntax.all._
import fs2.{RaiseThrowable, Stream}
import io.renku.graph.config.EventLogUrl
import io.renku.graph.eventlog.EventLogClient._
import io.renku.graph.model.eventlogapi._
import io.renku.graph.model.events._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.{Path => ProjectPath}
import io.renku.http.rest.paging.model.{Page, PerPage}
import org.http4s.Uri
import org.typelevel.log4cats.Logger
import scodec.bits.ByteVector

/** Client to the event-log microservice api. */
trait EventLogClient[F[_]] {

  def getEvents(criteria: SearchCriteria): F[Result[List[EventInfo]]]

  def getAllEvents(criteria: SearchCriteria)(implicit F: RaiseThrowable[F]): Stream[F, EventInfo] =
    Stream
      .eval(getEvents(criteria))
      .map(_.toEither)
      .rethrow
      .flatMap(Stream.emits) ++ getAllEvents(criteria.nextPage)

  def getEventPayload(eventId: EventId, projectPath: ProjectPath): F[Result[Option[EventPayload]]]

  def getStatus: F[Result[ServiceStatus]]
}

object EventLogClient {
  final case class EventPayload(data: ByteVector)

  sealed trait Result[+A] {
    def toEither: Either[Throwable, A]

  }
  object Result {
    final case class Success[+A](value: A) extends Result[A] {
      def toEither: Either[Throwable, A] = Right(value)
    }
    final case class Failure(error: String) extends RuntimeException(error) with Result[Nothing] {
      def toEither: Either[Throwable, Nothing] = Left(this)
    }

    final case object Unavailable extends RuntimeException("Service currently not available") with Result[Nothing] {
      def toEither: Either[Throwable, Nothing] = Left(this)
    }

    def success[A](value: A):      Result[A] = Success(value)
    def failure[A](error: String): Result[A] = Failure(error)
    def unavailable[A]: Result[A] = Unavailable
  }

  final case class SearchCriteria(
      projectAndStatus: Ior[projects.Identifier, EventStatus],
      since:            Option[EventDate],
      until:            Option[EventDate],
      page:             Page = Page.first,
      perPage:          Option[PerPage] = None,
      sort:             Option[SearchCriteria.Sort] = None
  ) {
    def withProject(id: projects.Identifier): SearchCriteria =
      copy(projectAndStatus = projectAndStatus match {
        case Ior.Left(_)       => Ior.Left(id)
        case Ior.Right(status) => Ior.both(id, status)
        case Ior.Both(_, s)    => Ior.both(id, s)
      })

    def setProjectPath(id: projects.Identifier): SearchCriteria =
      copy(projectAndStatus = Ior.left(id))

    def withStatus(status: EventStatus): SearchCriteria =
      copy(projectAndStatus = projectAndStatus match {
        case Ior.Left(path) => Ior.both(path, status)
        case Ior.Right(_)   => Ior.right(status)
        case Ior.Both(p, _) => Ior.both(p, status)
      })

    def setStatus(status: EventStatus): SearchCriteria =
      copy(projectAndStatus = Ior.right(status))

    def withPerPage(pp: PerPage): SearchCriteria =
      copy(perPage = Some(pp))

    def withUntil(date: EventDate): SearchCriteria =
      copy(until = Some(date))

    def withSince(date: EventDate): SearchCriteria =
      copy(since = Some(date))

    def sortBy(sort: SearchCriteria.Sort): SearchCriteria =
      copy(sort = Some(sort))

    def nextPage: SearchCriteria =
      copy(page = page.value + 1)

    val status: Option[EventStatus] = projectAndStatus.right
    val projectPath: Option[projects.Path] =
      projectAndStatus.left collect { case path: projects.Path => path }
    val projectId: Option[projects.GitLabId] =
      projectAndStatus.left collect { case id: projects.GitLabId => id }
  }

  object SearchCriteria {

    def forStatus(status: EventStatus): SearchCriteria =
      SearchCriteria(Ior.right(status), None, None)

    def forProject(id: projects.Identifier): SearchCriteria =
      SearchCriteria(Ior.left(id), None, None)

    sealed trait Sort
    object Sort {
      case object EventDateAsc  extends Sort
      case object EventDateDesc extends Sort
    }
  }

  def apply[F[_]: Async: Logger]: F[EventLogClient[F]] =
    EventLogUrl[F]().map(apply(_))

  def apply[F[_]: Async: Logger](baseUrl: EventLogUrl): EventLogClient[F] =
    new Http4sEventLogClient[F](Uri.unsafeFromString(baseUrl.value))
}
