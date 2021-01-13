/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions

import TestCategoryEvent._
import cats.effect.{IO, Timer}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import io.renku.eventlog.subscriptions.EventsSender.SendingResult
import io.renku.eventlog.subscriptions.EventsSender.SendingResult.{Delivered, Misdelivered, ServiceBusy}
import io.renku.eventlog.subscriptions.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.lang.Thread.sleep
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class EventsDistributorSpec extends AnyWordSpec with MockFactory with Eventually with should.Matchers {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(5, Seconds)),
    interval = scaled(Span(150, Millis))
  )

  "run" should {

    "continue dispatching events from the queue to some free subscribers" in new TestCase {

      val event           = testCategoryEvents.generateOne
      val subscriber      = subscriberUrls.generateOne
      val otherEvent      = testCategoryEvents.generateOne
      val otherSubscriber = subscriberUrls.generateOne

      inSequence {

        givenEventFinder(returns = Some(event))
        givenThereIs(freeSubscriber = subscriber)
        givenSending(event, to = subscriber, got = Delivered)
        givenDispatchRecoveryExists(subscriber, event)

        givenEventFinder(returns = Some(otherEvent))
        givenThereIs(freeSubscriber = otherSubscriber)
        givenSending(otherEvent, to = otherSubscriber, got = Delivered)
        givenDispatchRecoveryExists(otherSubscriber, otherEvent)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Info(s"$categoryName: $event, url = $subscriber -> $Delivered"),
          Info(s"$categoryName: $otherEvent, url = $otherSubscriber -> $Delivered")
        )
      }
    }

    "mark subscriber busy and dispatch an event to some other subscriber " +
      s"if delivery to the first one resulted in $ServiceBusy" in new TestCase {

        val event           = testCategoryEvents.generateOne
        val subscriber      = subscriberUrls.generateOne
        val otherSubscriber = subscriberUrls.generateOne

        inSequence {
          givenEventFinder(returns = Some(event))
          givenThereIs(freeSubscriber = subscriber)
          givenSending(event, to = subscriber, got = ServiceBusy)
          givenDispatchRecoveryExists(subscriber, event)
          expectMarkedBusy(subscriber)

          givenThereIs(freeSubscriber = otherSubscriber)
          givenSending(event, to = otherSubscriber, got = Delivered)
          givenDispatchRecoveryExists(otherSubscriber, event)

          givenNoMoreEvents()
        }

        distributor.run().unsafeRunAsyncAndForget()

        eventually {
          logger.loggedOnly(
            Info(s"$categoryName: $event, url = $otherSubscriber -> $Delivered")
          )
        }
      }

    s"remove subscriber which returned $Misdelivered on event dispatching " +
      "and use another subscriber if exists" in new TestCase {

        val event                = testCategoryEvents.generateOne
        val subscriber           = subscriberUrls.generateOne
        val otherSubscriber      = subscriberUrls.generateOne
        val yetAnotherSubscriber = subscriberUrls.generateOne

        inSequence {
          givenEventFinder(returns = Some(event))
          givenThereIs(freeSubscriber = subscriber)
          givenSending(event, to = subscriber, got = Misdelivered)
          givenDispatchRecoveryExists(subscriber, event)
          expectRemoval(subscriber)

          givenThereIs(freeSubscriber = otherSubscriber)
          givenSending(event, to = otherSubscriber, got = Misdelivered)
          givenDispatchRecoveryExists(otherSubscriber, event)
          expectRemoval(otherSubscriber)

          givenThereIs(freeSubscriber = yetAnotherSubscriber)
          givenSending(event, to = yetAnotherSubscriber, got = Delivered)
          givenDispatchRecoveryExists(yetAnotherSubscriber, event)

          givenNoMoreEvents()
        }

        sleep(500)

        distributor.run().unsafeRunAsyncAndForget()

        eventually {
          logger.loggedOnly(
            Error(s"$categoryName: $event, url = $subscriber -> $Misdelivered"),
            Error(s"$categoryName: $event, url = $otherSubscriber -> $Misdelivered"),
            Info(s"$categoryName: $event, url = $yetAnotherSubscriber -> $Delivered")
          )
        }
      }

    "recover using the given DispatchRecovery mechanism when sending the event failed " +
      "and continue processing some next event" in new TestCase {

        val subscriber   = subscriberUrls.generateOne
        val failingEvent = testCategoryEvents.generateOne
        val exception    = exceptions.generateOne
        val event        = testCategoryEvents.generateOne

        inSequence {
          givenEventFinder(returns = Some(failingEvent))
          givenThereIs(freeSubscriber = subscriber)

          (eventsSender.sendEvent _)
            .expects(subscriber, failingEvent)
            .returning(exception.raiseError[IO, SendingResult])

          givenDispatchRecoveryExists(subscriber, failingEvent)
          dispatchRecoveryStrategy
            .expects(exception)
            .returning(IO.unit)

          givenEventFinder(returns = Some(event))
          givenThereIs(freeSubscriber = subscriber)
          givenSending(event, to = subscriber, got = Delivered)
          givenDispatchRecoveryExists(subscriber, event)

          givenNoMoreEvents()
        }

        distributor.run().unsafeRunAsyncAndForget()

        eventually {
          logger.loggedOnly(
            Info(s"$categoryName: $event, url = $subscriber -> $Delivered")
          )
        }
      }

    "continue dispatching if a dispatch attempt fails" in new TestCase {

      val exception  = exceptions.generateOne
      val event      = testCategoryEvents.generateOne
      val subscriber = subscriberUrls.generateOne

      inSequence {
        givenEventFinder(returns = Some(event))

        (subscribers.runOnSubscriber _)
          .expects(*)
          .returning(exception.raiseError[IO, Unit])

        givenThereIs(freeSubscriber = subscriber)
        givenSending(event, to = subscriber, got = Delivered)
        givenDispatchRecoveryExists(subscriber, event)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"$categoryName: Dispatching an event failed", exception),
          Info(s"$categoryName: $event, url = $subscriber -> $Delivered")
        )
      }
    }

    "continue dispatching if finding event to process fails" in new TestCase {

      val exception  = exceptions.generateOne
      val event      = testCategoryEvents.generateOne
      val subscriber = subscriberUrls.generateOne

      inSequence {
        (eventsFinder.popEvent _)
          .expects()
          .returning(exception.raiseError[IO, Option[TestCategoryEvent]])

        (eventsFinder.popEvent _)
          .expects()
          .returning(exception.raiseError[IO, Option[TestCategoryEvent]])

        // retry fetching some new event
        givenEventFinder(returns = Some(event))
        givenThereIs(freeSubscriber = subscriber)
        givenSending(event, to = subscriber, got = Delivered)
        givenDispatchRecoveryExists(subscriber, event)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"$categoryName: Finding events to dispatch failed", exception),
          Error(s"$categoryName: Finding events to dispatch failed", exception),
          Info(s"$categoryName: $event, url = $subscriber -> $Delivered")
        )
      }
    }

    "re-dispatch the event if marking a subscriber busy fails" in new TestCase {

      val event      = testCategoryEvents.generateOne
      val exception  = exceptions.generateOne
      val subscriber = subscriberUrls.generateOne

      inSequence {
        givenEventFinder(returns = Some(event))

        givenThereIs(freeSubscriber = subscriber)
        givenSending(event, to = subscriber, got = ServiceBusy)
        givenDispatchRecoveryExists(subscriber, event)

        (subscribers.markBusy _)
          .expects(subscriber)
          .returning(exception.raiseError[IO, Unit])

        givenThereIs(freeSubscriber = subscriber)
        givenSending(event, to = subscriber, got = Delivered)
        givenDispatchRecoveryExists(subscriber, event)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Info(s"$categoryName: $event, url = $subscriber -> $Delivered")
        )
      }
    }

    "re-dispatch the event if removing a subscriber fails" in new TestCase {

      val event           = testCategoryEvents.generateOne
      val exception       = exceptions.generateOne
      val subscriber      = subscriberUrls.generateOne
      val otherSubscriber = subscriberUrls.generateOne

      inSequence {
        givenEventFinder(returns = Some(event))

        givenThereIs(freeSubscriber = subscriber)
        givenSending(event, to = subscriber, got = Misdelivered)
        givenDispatchRecoveryExists(subscriber, event)

        (subscribers.delete _)
          .expects(subscriber)
          .returning(exception.raiseError[IO, Unit])

        givenThereIs(freeSubscriber = otherSubscriber)
        givenSending(event, to = otherSubscriber, got = Delivered)
        givenDispatchRecoveryExists(otherSubscriber, event)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"$categoryName: $event, url = $subscriber -> $Misdelivered"),
          Info(s"$categoryName: $event, url = $otherSubscriber -> $Delivered")
        )
      }
    }
  }

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  private trait TestCase {

    val categoryName             = categoryNames.generateOne
    val subscribers              = mock[Subscribers[IO]]
    val eventsFinder             = mock[EventFinder[IO, TestCategoryEvent]]
    val eventsSender             = mock[EventsSender[IO, TestCategoryEvent]]
    private val dispatchRecovery = mock[DispatchRecovery[IO, TestCategoryEvent]]
    val logger                   = TestLogger[IO]()
    val distributor = new EventsDistributorImpl[IO, TestCategoryEvent](
      categoryName,
      subscribers,
      eventsFinder,
      eventsSender,
      dispatchRecovery,
      logger,
      noEventSleep = 250 millis,
      onErrorSleep = 250 millis
    )

    val dispatchRecoveryStrategy = mockFunction[Throwable, IO[Unit]]

    def givenDispatchRecoveryExists(subscriber: SubscriberUrl,
                                    event:      TestCategoryEvent,
                                    returning: PartialFunction[Throwable, IO[Unit]] =
                                      new PartialFunction[Throwable, IO[Unit]] {
                                        override def isDefinedAt(x:   Throwable) = true
                                        override def apply(throwable: Throwable) = dispatchRecoveryStrategy(throwable)
                                      }
    ) = (dispatchRecovery.recover _)
      .expects(subscriber, event)
      .returning(returning)

    def givenNoMoreEvents() =
      (eventsFinder.popEvent _)
        .expects()
        .returning(Option.empty[TestCategoryEvent].pure[IO])
        .anyNumberOfTimes()

    def givenThereIs(freeSubscriber: SubscriberUrl) =
      (subscribers.runOnSubscriber _).expects(*).onCall { f: (SubscriberUrl => IO[Unit]) =>
        f(freeSubscriber)
      }

    def expectRemoval(of: SubscriberUrl) =
      (subscribers.delete _)
        .expects(of)
        .returning(IO.unit)

    def expectMarkedBusy(subscriberUrl: SubscriberUrl) =
      (subscribers.markBusy _)
        .expects(subscriberUrl)
        .returning(IO.unit)

    def givenEventFinder(returns: Option[TestCategoryEvent]) =
      (eventsFinder.popEvent _)
        .expects()
        .returning(returns.pure[IO])

    def givenSending(event: TestCategoryEvent, to: SubscriberUrl, got: SendingResult): Any =
      (eventsSender.sendEvent _)
        .expects(to, event)
        .returning(got.pure[IO])
  }
}
