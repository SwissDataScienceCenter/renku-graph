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

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import fs2.{Chunk, Pipe}
import io.circe.Error
import io.circe.parser._
import io.renku.eventsqueue.{DequeuedEvent, EventsQueue}
import io.renku.graph.model.projects
import io.renku.triplesgenerator.api.events.{ProjectViewedEvent, UserId}
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private trait EventProcessor[F[_]] extends Pipe[F, Chunk[DequeuedEvent], Unit]

private object EventProcessor {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](connConfig:  ProjectsConnectionConfig,
                                                          eventsQueue: EventsQueue[F]
  ): F[EventProcessor[F]] =
    EventPersister[F](connConfig).map(new EventProcessorImpl[F](_, eventsQueue))
}

private class EventProcessorImpl[F[_]: MonadThrow: Logger](eventPersister: EventPersister[F],
                                                           eventsQueue: EventsQueue[F]
) extends EventProcessor[F] {

  override def apply(in: fs2.Stream[F, Chunk[DequeuedEvent]]): fs2.Stream[F, Unit] =
    in.evalMap(processChunk)

  private lazy val processChunk: Chunk[DequeuedEvent] => F[Unit] =
    _.toList
      .map(de => parseAndDecode(de).map(EventContext(de, _)))
      .sequence
      .map(_.groupBy(_.groupingKey).values.toList.map(_.sortBy(_.maybeDecoded.map(_.dateViewed)).reverse))
      .flatMap(_.traverse_(persist))

  private def parseAndDecode(de: DequeuedEvent): F[Option[ProjectViewedEvent]] =
    parse(de.payload)
      .flatMap(_.as[ProjectViewedEvent])
      .fold(logParsingFailure(de), Option(_).pure[F])

  private case class EventContext(dequeued: DequeuedEvent, maybeDecoded: Option[ProjectViewedEvent]) {
    lazy val groupingKey: Option[(projects.Slug, Option[UserId])] =
      maybeDecoded.map(ev => ev.slug -> ev.maybeUserId)
  }

  private def logParsingFailure(de: DequeuedEvent): Error => F[Option[ProjectViewedEvent]] =
    Logger[F].error(_)(show"$categoryName: decoding event failed: ${de.id}").as(Option.empty[ProjectViewedEvent])

  private lazy val persist: List[EventContext] => F[Unit] = { list =>
    list.headOption match {
      case Some(EventContext(de, Some(ev))) =>
        Logger[F].info(show"$categoryName: ${ev.slug} persisting group of ${list.size}") >>
          eventPersister.persist(ev).handleErrorWith(returnToQueue(de)(_))
      case _ =>
        ().pure[F]
    }
  }

  private def returnToQueue(de: DequeuedEvent): Throwable => F[Unit] =
    Logger[F].error(_)(show"$categoryName: persisting event failed: ${de.id}; returning to the queue") >>
      eventsQueue.returnToQueue(de)
}
