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

package io.renku.eventlog.events
package producers
package tsmigrationrequest

import EventsSender.SendingResult
import EventsSender.SendingResult._
import cats.effect.IO
import cats.syntax.all._
import io.renku.events.Subscription
import io.renku.events.Subscription._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import Generators.migrationRequestEvents

import scala.concurrent.duration._

class MigrationEventsDistributorSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with MockFactory
    with Eventually
    with IntegrationPatience {

  "run" should {

    "continue dispatching events from the queue to the subscriber from the event" in new TestCase {

      val event      = migrationRequestEvents.generateOne
      val otherEvent = migrationRequestEvents.generateOne

      inSequence {

        givenEventFinder(returns = Some(event))
        givenSending(event, to = event.subscriberUrl, got = Delivered)
        givenDispatchRecoveryExists(event)

        givenEventFinder(returns = Some(otherEvent))
        givenSending(otherEvent, to = otherEvent.subscriberUrl, got = Delivered)
        givenDispatchRecoveryExists(otherEvent)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAndForget()

      eventually {
        logger.loggedOnly(
          Info(show"$categoryName: $event, subscriber = ${event.subscriberUrl} -> $Delivered"),
          Info(show"$categoryName: $otherEvent, subscriber = ${otherEvent.subscriberUrl} -> $Delivered")
        )
      }
    }

    "return the event back to the queue " +
      s"if delivery resulted in $TemporarilyUnavailable" in new TestCase {

        val event = migrationRequestEvents.generateOne

        inSequence {
          givenEventFinder(returns = Some(event))
          givenSending(event, to = event.subscriberUrl, got = TemporarilyUnavailable)
          givenDispatchRecoveryExists(event)
          expectEventReturnedToTheQueue(event, reason = TemporarilyUnavailable, got = ().pure[IO])

          givenEventFinder(returns = Some(event))
          givenSending(event, to = event.subscriberUrl, got = Delivered)
          givenDispatchRecoveryExists(event)

          givenNoMoreEvents()
        }

        distributor.run().unsafeRunAndForget()

        eventually {
          logger.loggedOnly(
            Info(show"$categoryName: $event, subscriber = ${event.subscriberUrl} -> $Delivered")
          )
        }
      }

    s"remove subscriber which returned $Misdelivered on dispatching " +
      "and return the event to the queue" in new TestCase {

        val failingEvent    = migrationRequestEvents.generateOne
        val succeedingEvent = migrationRequestEvents.generateOne

        inSequence {
          givenEventFinder(returns = Some(failingEvent))
          givenSending(failingEvent, to = failingEvent.subscriberUrl, got = Misdelivered)
          givenDispatchRecoveryExists(failingEvent)
          expectRemoval(failingEvent.subscriberUrl)

          givenEventFinder(returns = Some(succeedingEvent))
          givenSending(succeedingEvent, to = succeedingEvent.subscriberUrl, got = Delivered)
          givenDispatchRecoveryExists(succeedingEvent)

          givenNoMoreEvents()
        }

        distributor.run().unsafeRunAndForget()

        eventually {
          logger.loggedOnly(
            Info(show"$categoryName: $failingEvent, subscriber = ${failingEvent.subscriberUrl} -> $Misdelivered"),
            Info(show"$categoryName: $succeedingEvent, subscriber = ${succeedingEvent.subscriberUrl} -> $Delivered")
          )
        }
      }

    "recover using the given DispatchRecovery mechanism when sending an event fails " +
      "and continue processing some next event" in new TestCase {

        val failingEvent    = migrationRequestEvents.generateOne
        val exception       = exceptions.generateOne
        val succeedingEvent = migrationRequestEvents.generateOne

        inSequence {

          givenEventFinder(returns = Some(failingEvent))

          (eventsSender.sendEvent _)
            .expects(failingEvent.subscriberUrl, failingEvent)
            .returning(exception.raiseError[IO, SendingResult])

          givenDispatchRecoveryExists(failingEvent)
          dispatchRecoveryStrategy
            .expects(exception)
            .returning(IO.unit)

          givenEventFinder(returns = Some(succeedingEvent))
          givenSending(succeedingEvent, to = succeedingEvent.subscriberUrl, got = Delivered)
          givenDispatchRecoveryExists(succeedingEvent)

          givenNoMoreEvents()
        }

        distributor.run().unsafeRunAndForget()

        eventually {
          logger.loggedOnly(
            Info(show"$categoryName: $succeedingEvent, subscriber = ${succeedingEvent.subscriberUrl} -> $Delivered")
          )
        }
      }

    "log an error if finding an event fails" in new TestCase {

      val exception = exceptions.generateOne
      val event     = migrationRequestEvents.generateOne

      inSequence {

        (eventsFinder.popEvent _)
          .expects()
          .returning(exception.raiseError[IO, Option[MigrationRequestEvent]])

        givenEventFinder(returns = Some(event))
        givenSending(event, to = event.subscriberUrl, got = Delivered)
        givenDispatchRecoveryExists(event)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAndForget()

      eventually {
        logger.loggedOnly(
          Error(show"$categoryName: finding events to dispatch failed", exception),
          Info(show"$categoryName: $event, subscriber = ${event.subscriberUrl} -> $Delivered")
        )
      }
    }

    "log an error if removing a subscriber fails" in new TestCase {

      val event      = migrationRequestEvents.generateOne
      val exception  = exceptions.generateOne
      val otherEvent = migrationRequestEvents.generateOne

      inSequence {
        givenEventFinder(returns = Some(event))
        givenSending(event, to = event.subscriberUrl, got = Misdelivered)
        givenDispatchRecoveryExists(event)
        (subscribers.delete _)
          .expects(event.subscriberUrl)
          .returning(exception.raiseError[IO, Unit])

        givenEventFinder(returns = Some(otherEvent))
        givenSending(otherEvent, to = otherEvent.subscriberUrl, got = Delivered)
        givenDispatchRecoveryExists(otherEvent)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAndForget()

      eventually {
        logger.loggedOnly(
          Info(show"$categoryName: $event, subscriber = ${event.subscriberUrl} -> $Misdelivered"),
          Error(show"$categoryName: $event -> deleting subscriber failed", exception),
          Info(show"$categoryName: $otherEvent, subscriber = ${otherEvent.subscriberUrl} -> $Delivered")
        )
      }
    }

    "log an error when returning event back to the queue fails" in new TestCase {

      val event                   = migrationRequestEvents.generateOne
      val backToTheQueueException = exceptions.generateOne
      val otherEvent              = migrationRequestEvents.generateOne

      inSequence {

        givenEventFinder(returns = Some(event))
        givenSending(event, to = event.subscriberUrl, got = TemporarilyUnavailable)
        givenDispatchRecoveryExists(event)
        expectEventReturnedToTheQueue(event,
                                      reason = TemporarilyUnavailable,
                                      got = backToTheQueueException.raiseError[IO, Unit]
        )

        givenEventFinder(returns = Some(otherEvent))
        givenSending(otherEvent, to = otherEvent.subscriberUrl, got = Delivered)
        givenDispatchRecoveryExists(otherEvent)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAndForget()

      eventually {
        logger.loggedOnly(
          Error(show"$categoryName: $event -> returning event to the queue failed", backToTheQueueException),
          Info(show"$categoryName: $otherEvent, subscriber = ${otherEvent.subscriberUrl} -> $Delivered")
        )
      }
    }
  }

  private trait TestCase {

    val subscribers              = mock[Subscribers[IO, Subscription.Subscriber]]
    val eventsFinder             = mock[EventFinder[IO]]
    val eventsSender             = mock[EventsSender[IO, MigrationRequestEvent]]
    private val dispatchRecovery = mock[DispatchRecovery[IO, MigrationRequestEvent]]
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val distributor = new MigrationEventsDistributorImpl[IO](
      categoryName,
      subscribers,
      eventsFinder,
      eventsSender,
      dispatchRecovery,
      noEventSleep = 250 millis,
      onErrorSleep = 250 millis
    )

    val dispatchRecoveryStrategy = mockFunction[Throwable, IO[Unit]]

    def expectEventReturnedToTheQueue(event: MigrationRequestEvent, reason: SendingResult, got: IO[Unit]) =
      (dispatchRecovery.returnToQueue _)
        .expects(event, reason)
        .returning(got)

    def givenDispatchRecoveryExists(event: MigrationRequestEvent,
                                    returning: PartialFunction[Throwable, IO[Unit]] =
                                      new PartialFunction[Throwable, IO[Unit]] {
                                        override def isDefinedAt(x: Throwable) = true

                                        override def apply(throwable: Throwable) = dispatchRecoveryStrategy(throwable)
                                      }
    ) = (dispatchRecovery.recover _)
      .expects(event.subscriberUrl, event)
      .returning(returning)

    def givenNoMoreEvents() =
      (eventsFinder.popEvent _)
        .expects()
        .returning(Option.empty[MigrationRequestEvent].pure[IO])
        .anyNumberOfTimes()

    def expectRemoval(of: SubscriberUrl) =
      (subscribers.delete _)
        .expects(of)
        .returning(IO.unit)

    def givenEventFinder(returns: Option[MigrationRequestEvent]) =
      (eventsFinder.popEvent _)
        .expects()
        .returning(returns.pure[IO])

    def givenSending(event: MigrationRequestEvent, to: SubscriberUrl, got: SendingResult): Any =
      (eventsSender.sendEvent _)
        .expects(to, event)
        .returning(got.pure[IO])
  }
}
