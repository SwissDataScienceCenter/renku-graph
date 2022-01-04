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

package io.renku.eventlog.events.categories.statuschange

import cats.effect.{IO, Ref}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.syntax._
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog.events.categories.statuschange.Generators.projectEventToNewEvents
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ProjectEventsToNew
import io.renku.eventlog.{EventLogDB, InMemoryEventLogDbSpec}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings, positiveInts}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import natchez.Trace.Implicits.noop
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.Session

class StatusChangeEventsQueueSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryEventLogDbSpec
    with Eventually
    with IntegrationPatience {

  "run" should {

    "keep dequeueing items from the status_change_events_queue" in new TestCase {

      val event = projectEventToNewEvents.generateOne

      sessionResource.useK(queue offer event).unsafeRunSync()

      queue.run().unsafeRunAndForget()

      queue.register(eventHandler).unsafeRunSync()

      eventually {
        dequeuedEvents.get.unsafeRunSync() shouldBe List(event)
      }

      val nextEvent = projectEventToNewEvents.generateOne
      sessionResource.useK(queue offer nextEvent).unsafeRunSync()

      eventually {
        dequeuedEvents.get.unsafeRunSync() shouldBe List(event, nextEvent)
      }
    }

    "be dequeueing the oldest events first" in new TestCase {
      val event = projectEventToNewEvents.generateOne
      sessionResource.useK(queue offer event).unsafeRunSync()

      val nextEvent = projectEventToNewEvents.generateOne
      sessionResource.useK(queue offer nextEvent).unsafeRunSync()

      queue.register(eventHandler).unsafeRunSync()

      queue.run().unsafeRunAndForget()

      eventually {
        dequeuedEvents.get.unsafeRunSync() shouldBe List(event, nextEvent)
      }
    }

    "log error and continue dequeueing next events if handler fails during processing event" in new TestCase {
      val event = projectEventToNewEvents.generateOne
      sessionResource.useK(queue offer event).unsafeRunSync()
      val nextEvent = projectEventToNewEvents.generateOne
      sessionResource.useK(queue offer nextEvent).unsafeRunSync()

      val exception = exceptions.generateOne
      val failingHandler: ProjectEventsToNew => IO[Unit] = {
        case `event` => exception.raiseError[IO, Unit]
        case other   => dequeuedEvents.update(_ ::: other :: Nil)
      }

      queue.register(failingHandler).unsafeRunSync()

      queue.run().unsafeRunAndForget()

      eventually {
        logger.loggedOnly(
          Error(
            show"$categoryName processing event ${event.asJson.noSpaces} of type ${ProjectEventsToNew.eventType} failed",
            exception
          )
        )
        dequeuedEvents.get.unsafeRunSync() shouldBe List(nextEvent)
      }
    }

    "continue if there are general problems with dequeueing" in {
      val failingSessionResource = new SessionResource[IO, EventLogDB](
        Session.single[IO](
          host = container.host,
          port = positiveInts().generateOne.value,
          user = nonEmptyStrings().generateOne,
          database = nonEmptyStrings().generateOne,
          password = Some(nonEmptyStrings().generateOne)
        )
      )
      implicit val logger: TestLogger[IO] = TestLogger[IO]()
      val queryExec = TestLabeledHistogram[SqlStatement.Name]("query_id")
      val queue     = new StatusChangeEventsQueueImpl[IO](failingSessionResource, queryExec)

      val handler: ProjectEventsToNew => IO[Unit] = _ => ().pure[IO]
      queue.register(handler).unsafeRunSync()

      queue.run().unsafeRunAndForget()

      eventually {
        logger
          .getMessages(Error)
          .map(_.message)
          .count(_ contains show"$categoryName processing events from the queue failed") should be > 1
      }
    }
  }

  private trait TestCase {

    val dequeuedEvents: Ref[IO, List[ProjectEventsToNew]] = Ref.unsafe[IO, List[ProjectEventsToNew]](List.empty)
    val eventHandler:   ProjectEventsToNew => IO[Unit]    = e => dequeuedEvents.update(_ ::: e :: Nil)

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val queryExec = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val queue     = new StatusChangeEventsQueueImpl[IO](sessionResource, queryExec)
  }
}
