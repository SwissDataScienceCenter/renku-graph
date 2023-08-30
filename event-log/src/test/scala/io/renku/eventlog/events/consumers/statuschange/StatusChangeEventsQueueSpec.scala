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

import cats.effect.{Async, IO, Ref}
import cats.syntax.all._
import io.circe.syntax._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.api.events.StatusChangeEvent.ProjectEventsToNew
import io.renku.eventlog.api.events.StatusChangeGenerators
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.eventlog.{EventLogDB, InMemoryEventLogDbSpec}
import io.renku.generators.Generators
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import natchez.Trace.Implicits.noop
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import projecteventstonew.eventType
import skunk.Session

class StatusChangeEventsQueueSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryEventLogDbSpec
    with Eventually
    with IntegrationPatience {

  "run" should {

    "keep dequeuing items from the status_change_events_queue" in new TestCase {

      val event = StatusChangeGenerators.projectEventsToNewEvents.generateOne

      sessionResource.useK(queue offer event).unsafeRunSync()

      queue.run.unsafeRunAndForget()

      queue.register(projecteventstonew.eventType, eventHandler).unsafeRunSync()

      eventually {
        dequeuedEvents.get.unsafeRunSync() shouldBe List(event)
      }

      val nextEvent = StatusChangeGenerators.projectEventsToNewEvents.generateOne
      sessionResource.useK(queue offer nextEvent).unsafeRunSync()

      eventually {
        dequeuedEvents.get.unsafeRunSync() shouldBe List(event, nextEvent)
      }
    }

    "be dequeueing the oldest events first" in new TestCase {
      val event = StatusChangeGenerators.projectEventsToNewEvents.generateOne
      sessionResource.useK(queue offer event).unsafeRunSync()

      val nextEvent = StatusChangeGenerators.projectEventsToNewEvents.generateOne
      sessionResource.useK(queue offer nextEvent).unsafeRunSync()

      queue.register(projecteventstonew.eventType, eventHandler).unsafeRunSync()

      queue.run.unsafeRunAndForget()

      eventually {
        dequeuedEvents.get.unsafeRunSync() shouldBe List(event, nextEvent)
      }
    }

    "log error and continue dequeueing next events if handler fails during processing event" in new TestCase {
      val event = StatusChangeGenerators.projectEventsToNewEvents.generateOne
      sessionResource.useK(queue offer event).unsafeRunSync()
      val nextEvent = StatusChangeGenerators.projectEventsToNewEvents.generateOne
      sessionResource.useK(queue offer nextEvent).unsafeRunSync()

      val exception = Generators.exceptions.generateOne
      val failingHandler: ProjectEventsToNew => IO[Unit] = {
        case `event` => IO.raiseError(exception)
        case other   => dequeuedEvents.update(_ ::: other :: Nil)
      }

      queue.register(projecteventstonew.eventType, failingHandler).unsafeRunSync()

      queue.run.unsafeRunAndForget()

      eventually {
        logger.loggedOnly(
          TestLogger.Level.Error(
            show"$categoryName processing event ${event.asJson.noSpaces} of type ${projecteventstonew.eventType} failed",
            exception
          )
        )
        dequeuedEvents.get.unsafeRunSync() shouldBe List(nextEvent)
      }
    }

    "continue if there are general problems with dequeueing" in {
      implicit val failingSessionResource: SessionResource[IO] = io.renku.db.SessionResource[IO, EventLogDB](
        Session.single[IO](
          host = dbConfig.host.value,
          port = Generators.positiveInts().generateOne.value,
          user = Generators.nonEmptyStrings().generateOne,
          database = Generators.nonEmptyStrings().generateOne,
          password = Some(Generators.nonEmptyStrings().generateOne)
        )
      )
      implicit val logger:           TestLogger[IO]            = TestLogger[IO]()
      implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
      implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
      val queue =
        new StatusChangeEventsQueueImpl[IO]()(implicitly[Async[IO]], logger, failingSessionResource, queriesExecTimes)

      val handler: ProjectEventsToNew => IO[Unit] = _ => ().pure[IO]
      queue.register(projecteventstonew.eventType, handler).unsafeRunSync()

      queue.run.unsafeRunAndForget()

      eventually {
        logger
          .getMessages(TestLogger.Level.Error)
          .map(_.message)
          .count(_ contains show"$categoryName processing events from the queue failed") should be > 1
      }
    }
  }

  private trait TestCase {

    val dequeuedEvents: Ref[IO, List[ProjectEventsToNew]] = Ref.unsafe[IO, List[ProjectEventsToNew]](List.empty)
    val eventHandler:   ProjectEventsToNew => IO[Unit]    = e => dequeuedEvents.update(_ ::: e :: Nil)

    implicit val logger:                   TestLogger[IO]            = TestLogger[IO]()
    private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val queue = new StatusChangeEventsQueueImpl[IO]
  }
}
