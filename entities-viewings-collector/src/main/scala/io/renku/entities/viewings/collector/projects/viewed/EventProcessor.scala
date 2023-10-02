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

package io.renku.entities.viewings.collector.projects.viewed

import cats.effect.Async
import cats.syntax.all._
import cats.{Eq, MonadThrow}
import fs2.{Pipe, Stream}
import io.circe.Error
import io.circe.parser._
import io.renku.eventsqueue.DequeuedEvent
import io.renku.graph.model.projects
import io.renku.triplesgenerator.api.events.{ProjectViewedEvent, UserId}
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private trait EventProcessor[F[_]] extends Pipe[F, DequeuedEvent, DequeuedEvent]

private object EventProcessor {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](connConfig: ProjectsConnectionConfig): F[EventProcessor[F]] =
    EventPersister[F](connConfig).map(new EventProcessorImpl[F](_))
}

private class EventProcessorImpl[F[_]: MonadThrow: Logger](eventPersister: EventPersister[F])
    extends EventProcessor[F] {

  override def apply(in: fs2.Stream[F, DequeuedEvent]): fs2.Stream[F, DequeuedEvent] =
    in
      .evalMap(de => parseAndDecode(de).fproductLeft(_ => de))
      .groupAdjacentBy { case (_, maybeEv) => maybeEv.map(ev => ev.slug -> ev.maybeUserId) }
      .map { case (slug, chunk) => slug -> chunk.toList.sortBy { case (_, maybeEv) => maybeEv.map(_.dateViewed) } }
      .evalMap { case (_, sorted) => persist(sorted) }
      .flatMap(events => Stream.emits[F, DequeuedEvent](events.map(_._1)))

  private lazy val parseAndDecode: DequeuedEvent => F[Option[ProjectViewedEvent]] =
    de =>
      parse(de.payload)
        .flatMap(_.as[ProjectViewedEvent])
        .fold(logParsingFailure(de), Option(_).pure[F])

  private def logParsingFailure(de: DequeuedEvent): Error => F[Option[ProjectViewedEvent]] =
    Logger[F].error(_)(show"Decoding event failed: $de").as(Option.empty[ProjectViewedEvent])

  private implicit lazy val groupByEq: Eq[(projects.Slug, Option[UserId])] = Eq.instance {
    case (slug1 -> maybeUserId1, slug2 -> maybeUserId2) =>
      slug1 == slug2 && maybeUserId1 == maybeUserId2
  }

  private lazy val persist
      : List[(DequeuedEvent, Option[ProjectViewedEvent])] => F[List[(DequeuedEvent, Option[ProjectViewedEvent])]] = {
    list =>
      list.reverse.headOption match {
        case Some(de -> Some(ev)) =>
          Logger[F].info(show"$categoryName: ${ev.slug} persisting group of ${list.size}") >>
            eventPersister.persist(ev).as(list).handleErrorWith(logPersistingFailure(de)(_).as(list.reverse.tail))
        case _ =>
          list.pure[F]
      }
  }

  private def logPersistingFailure(de: DequeuedEvent): Throwable => F[Unit] =
    Logger[F].error(_)(show"Persisting event failed: $de")
}
