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

package io.renku.eventlog.events.consumers.statuschange

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{Async, IO, Temporal}
import cats.syntax.all._
import fs2.concurrent.SignallingRef
import io.circe.syntax._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.api.events.StatusChangeEvent.ProjectEventsToNew
import io.renku.eventlog.api.events.StatusChangeGenerators.projectEventsToNewEvents
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.generators.Generators
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger.Level.Error
import natchez.Trace.Implicits.noop
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import projecteventstonew.eventType
import skunk.Session

import scala.concurrent.duration._

class StatusChangeEventsQueueSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  it should "keep dequeuing items from the status_change_events_queue" in testDBResource.use { implicit cfg =>
    val event = projectEventsToNewEvents.generateOne
    val queue = initQueue
    for {
      _ <- moduleSessionResource.session.useKleisli(queue offer event)

      _ <- queue.run.start

      eventsCollector <- eventsCollectorIO
      _               <- queue.register(projecteventstonew.eventType, eventHandler(eventsCollector)).assertNoException

      _ <- eventsCollector.waitUntil(_ == List(event))

      nextEvent = projectEventsToNewEvents.generateOne
      _ <- moduleSessionResource.session.useKleisli(queue offer nextEvent)

      _ <- eventsCollector.waitUntil(_ == List(event, nextEvent))
    } yield Succeeded
  }

  it should "be dequeueing the oldest events first" in testDBResource.use { implicit cfg =>
    val event = projectEventsToNewEvents.generateOne
    val queue = initQueue
    for {
      _ <- moduleSessionResource.session.useKleisli(queue offer event)

      nextEvent = projectEventsToNewEvents.generateOne
      _ <- moduleSessionResource.session.useKleisli(queue offer nextEvent)

      eventsCollector <- eventsCollectorIO
      _               <- queue.register(projecteventstonew.eventType, eventHandler(eventsCollector))

      _ <- queue.run.start

      _ <- eventsCollector.waitUntil(_ == List(event, nextEvent))
    } yield Succeeded
  }

  it should "log error and continue dequeueing next events if handler fails during processing event" in testDBResource
    .use { implicit cfg =>
      val event = projectEventsToNewEvents.generateOne
      val queue = initQueue
      for {
        _ <- moduleSessionResource.session.useKleisli(queue offer event)
        nextEvent = projectEventsToNewEvents.generateOne
        _ <- moduleSessionResource.session.useKleisli(queue offer nextEvent)

        eventsCollector <- eventsCollectorIO
        exception = Generators.exceptions.generateOne
        handler = {
                    case `event` => IO.raiseError(exception)
                    case other   => eventsCollector.update(_ ::: other :: Nil)
                  }: ProjectEventsToNew => IO[Unit]
        _ <- queue.register(projecteventstonew.eventType, handler)

        _ <- logger.resetF()
        _ <- queue.run.start

        _ <- eventsCollector.waitUntil(_ == List(nextEvent))
        _ <-
          logger.loggedOnlyF(
            Error(
              show"$categoryName processing event ${event.asJson.noSpaces} of type ${projecteventstonew.eventType} failed",
              exception
            )
          )
      } yield Succeeded
    }

  it should "continue if there are general problems with dequeueing" in testDBResource.use { implicit cfg =>
    val failingSessionResource: SessionResource[IO] = io.renku.db.SessionResource[IO, EventLogDB](
      Session.single[IO](
        host = cfg.host.value,
        port = Generators.positiveInts().generateOne.value,
        user = Generators.nonEmptyStrings().generateOne,
        database = Generators.nonEmptyStrings().generateOne,
        password = Some(Generators.nonEmptyStrings().generateOne)
      ),
      cfg
    )
    val queue = new StatusChangeEventsQueueImpl[IO](queueCheckInterval = 250 millis)(implicitly[Async[IO]],
                                                                                     logger,
                                                                                     failingSessionResource,
                                                                                     qet
    )
    def assureTheProcessRecovers: IO[Unit] =
      logger
        .getMessagesF(Error)
        .map(_.map(_.message).count(_ contains show"$categoryName processing events from the queue failed"))
        .flatMap {
          case count if count > 1 => ().pure[IO]
          case _                  => Temporal[IO].delayBy(assureTheProcessRecovers, 250 millis)
        }

    for {
      _ <- queue.register(projecteventstonew.eventType, handler = (_: ProjectEventsToNew) => ().pure[IO])
      _ <- logger.resetF()
      _ <- queue.run.start
      _ <- assureTheProcessRecovers
    } yield Succeeded
  }

  private def eventsCollectorIO: IO[SignallingRef[IO, List[ProjectEventsToNew]]] =
    SignallingRef.of[IO, List[ProjectEventsToNew]](List.empty)
  private def eventHandler(collector: SignallingRef[IO, List[ProjectEventsToNew]]): ProjectEventsToNew => IO[Unit] =
    e => collector.update(_ ::: e :: Nil)

  private implicit lazy val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
  private def initQueue(implicit cfg: DBConfig[EventLogDB]) =
    new StatusChangeEventsQueueImpl[IO](queueCheckInterval = 250 millis)
}
