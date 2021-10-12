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

import cats.effect.{IO, Timer}
import cats.syntax.all._
import ch.datascience.events.consumers.subscriptions._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import io.renku.eventlog.subscriptions.EventsSender.SendingResult
import io.renku.eventlog.subscriptions.EventsSender.SendingResult._
import io.renku.eventlog.subscriptions.Generators._
import io.renku.eventlog.subscriptions.TestCategoryEvent._
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

        givenThereIs(freeSubscriber = subscriber)
        givenEventFinder(returns = Some(event))
        givenSending(event, to = subscriber, got = Delivered)
        givenDispatchRecoveryExists(subscriber, event)
        expectSendingRegistered(event, subscriber)

        givenThereIs(freeSubscriber = otherSubscriber)
        givenEventFinder(returns = Some(otherEvent))
        givenSending(otherEvent, to = otherSubscriber, got = Delivered)
        givenDispatchRecoveryExists(otherSubscriber, otherEvent)
        expectSendingRegistered(otherEvent, otherSubscriber)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Info(s"$categoryName: $event, subscriber = $subscriber -> $Delivered"),
          Info(s"$categoryName: $otherEvent, subscriber = $otherSubscriber -> $Delivered")
        )
      }
    }

    "mark subscriber busy and return the event back to the queue " +
      s"if delivery resulted in $TemporarilyUnavailable" in new TestCase {

        val event           = testCategoryEvents.generateOne
        val subscriber      = subscriberUrls.generateOne
        val otherSubscriber = subscriberUrls.generateOne

        inSequence {
          givenThereIs(freeSubscriber = subscriber)
          givenEventFinder(returns = Some(event))
          givenSending(event, to = subscriber, got = TemporarilyUnavailable)
          givenDispatchRecoveryExists(subscriber, event)
          expectMarkedBusy(subscriber)
          expectEventReturnedToTheQueue(event, got = ().pure[IO])

          givenThereIs(freeSubscriber = otherSubscriber)
          givenEventFinder(returns = Some(event))
          givenSending(event, to = otherSubscriber, got = Delivered)
          givenDispatchRecoveryExists(otherSubscriber, event)
          expectSendingRegistered(event, otherSubscriber)

          givenNoMoreEvents()
        }

        distributor.run().unsafeRunAsyncAndForget()

        eventually {
          logger.loggedOnly(
            Info(s"$categoryName: $event, subscriber = $otherSubscriber -> $Delivered")
          )
        }
      }

    s"remove subscriber which returned $Misdelivered on event dispatching " +
      "and return the event back to the queue" in new TestCase {

        val event                = testCategoryEvents.generateOne
        val subscriber           = subscriberUrls.generateOne
        val otherSubscriber      = subscriberUrls.generateOne
        val yetAnotherSubscriber = subscriberUrls.generateOne

        inSequence {
          givenThereIs(freeSubscriber = subscriber)
          givenEventFinder(returns = Some(event))
          givenSending(event, to = subscriber, got = Misdelivered)
          givenDispatchRecoveryExists(subscriber, event)
          expectRemoval(subscriber)
          expectEventReturnedToTheQueue(event = event, got = ().pure[IO])

          givenThereIs(freeSubscriber = otherSubscriber)
          givenEventFinder(returns = Some(event))
          givenSending(event, to = otherSubscriber, got = Misdelivered)
          givenDispatchRecoveryExists(otherSubscriber, event)
          expectRemoval(otherSubscriber)
          expectEventReturnedToTheQueue(event = event, got = ().pure[IO])

          givenThereIs(freeSubscriber = yetAnotherSubscriber)
          givenEventFinder(returns = Some(event))
          givenSending(event, to = yetAnotherSubscriber, got = Delivered)
          givenDispatchRecoveryExists(yetAnotherSubscriber, event)
          expectSendingRegistered(event, yetAnotherSubscriber)

          givenNoMoreEvents()
        }

        sleep(500)

        distributor.run().unsafeRunAsyncAndForget()

        eventually {
          logger.loggedOnly(
            Error(s"$categoryName: $event, subscriber = $subscriber -> $Misdelivered"),
            Error(s"$categoryName: $event, subscriber = $otherSubscriber -> $Misdelivered"),
            Info(s"$categoryName: $event, subscriber = $yetAnotherSubscriber -> $Delivered")
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
          givenThereIs(freeSubscriber = subscriber)
          givenEventFinder(returns = Some(failingEvent))

          (eventsSender.sendEvent _)
            .expects(subscriber, failingEvent)
            .returning(exception.raiseError[IO, SendingResult])

          givenDispatchRecoveryExists(subscriber, failingEvent)
          dispatchRecoveryStrategy
            .expects(exception)
            .returning(IO.unit)

          givenThereIs(freeSubscriber = subscriber)
          givenEventFinder(returns = Some(event))
          givenSending(event, to = subscriber, got = Delivered)
          givenDispatchRecoveryExists(subscriber, event)
          expectSendingRegistered(event, subscriber)

          givenNoMoreEvents()
        }

        distributor.run().unsafeRunAsyncAndForget()

        eventually {
          logger.loggedOnly(
            Info(s"$categoryName: $event, subscriber = $subscriber -> $Delivered")
          )
        }
      }

    "continue dispatching if a dispatch attempt fails" in new TestCase {

      val exception  = exceptions.generateOne
      val event      = testCategoryEvents.generateOne
      val subscriber = subscriberUrls.generateOne

      inSequence {
        (subscribers.runOnSubscriber _)
          .expects(*)
          .returning(exception.raiseError[IO, Unit])

        givenThereIs(freeSubscriber = subscriber)
        givenEventFinder(returns = Some(event))
        givenSending(event, to = subscriber, got = Delivered)
        givenDispatchRecoveryExists(subscriber, event)
        expectSendingRegistered(event, subscriber)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"$categoryName: executing event distribution on a subscriber failed", exception),
          Info(s"$categoryName: $event, subscriber = $subscriber -> $Delivered")
        )
      }
    }

    "log an error if pop event to process fails" in new TestCase {

      val exception  = exceptions.generateOne
      val event      = testCategoryEvents.generateOne
      val subscriber = subscriberUrls.generateOne

      inSequence {
        givenThereIs(freeSubscriber = subscriber)

        (eventsFinder.popEvent _)
          .expects()
          .returning(exception.raiseError[IO, Option[TestCategoryEvent]])

        givenThereIs(freeSubscriber = subscriber)
        givenEventFinder(returns = Some(event))
        givenSending(event, to = subscriber, got = Delivered)
        givenDispatchRecoveryExists(subscriber, event)
        expectSendingRegistered(event, subscriber)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"$categoryName: finding events to dispatch failed", exception),
          Info(s"$categoryName: $event, subscriber = $subscriber -> $Delivered")
        )
      }
    }

    "log an error and look for a new event if registering event delivery fails" in new TestCase {

      val exception  = exceptions.generateOne
      val event      = testCategoryEvents.generateOne
      val otherEvent = testCategoryEvents.generateOne
      val subscriber = subscriberUrls.generateOne

      inSequence {
        givenThereIs(freeSubscriber = subscriber)
        givenEventFinder(returns = Some(event))
        givenSending(event, to = subscriber, got = Delivered)
        givenDispatchRecoveryExists(subscriber, event)
        (eventDelivery.registerSending _)
          .expects(event, subscriber)
          .returning(exception.raiseError[IO, Unit])

        givenThereIs(freeSubscriber = subscriber)
        givenEventFinder(returns = Some(otherEvent))
        givenSending(otherEvent, to = subscriber, got = Delivered)
        givenDispatchRecoveryExists(subscriber, otherEvent)
        expectSendingRegistered(otherEvent, subscriber)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Info(s"$categoryName: $event, subscriber = $subscriber -> $Delivered"),
          Error(s"$categoryName: registering sending $event to $subscriber failed", exception),
          Info(s"$categoryName: $otherEvent, subscriber = $subscriber -> $Delivered")
        )
      }
    }

    "return the event back to the queue if marking subscriber as busy fails" in new TestCase {

      val event      = testCategoryEvents.generateOne
      val exception  = exceptions.generateOne
      val subscriber = subscriberUrls.generateOne

      inSequence {
        givenThereIs(freeSubscriber = subscriber)
        givenEventFinder(returns = Some(event))
        givenSending(event, to = subscriber, got = TemporarilyUnavailable)
        givenDispatchRecoveryExists(subscriber, event)
        (subscribers.markBusy _)
          .expects(subscriber)
          .returning(exception.raiseError[IO, Unit])
        expectEventReturnedToTheQueue(event, got = ().pure[IO])

        givenThereIs(freeSubscriber = subscriber)
        givenEventFinder(returns = Some(event))
        givenSending(event, to = subscriber, got = Delivered)
        givenDispatchRecoveryExists(subscriber, event)
        expectSendingRegistered(event, subscriber)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Info(s"$categoryName: $event, subscriber = $subscriber -> $Delivered")
        )
      }
    }

    "return the event back to the queue if removing a subscriber fails" in new TestCase {

      val event           = testCategoryEvents.generateOne
      val exception       = exceptions.generateOne
      val subscriber      = subscriberUrls.generateOne
      val otherSubscriber = subscriberUrls.generateOne

      inSequence {
        givenThereIs(freeSubscriber = subscriber)
        givenEventFinder(returns = Some(event))
        givenSending(event, to = subscriber, got = Misdelivered)
        givenDispatchRecoveryExists(subscriber, event)
        (subscribers.delete _)
          .expects(subscriber)
          .returning(exception.raiseError[IO, Unit])
        expectEventReturnedToTheQueue(event, got = ().pure[IO])

        givenThereIs(freeSubscriber = otherSubscriber)
        givenEventFinder(returns = Some(event))
        givenSending(event, to = otherSubscriber, got = Delivered)
        givenDispatchRecoveryExists(otherSubscriber, event)
        expectSendingRegistered(event, otherSubscriber)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"$categoryName: $event, subscriber = $subscriber -> $Misdelivered"),
          Info(s"$categoryName: $event, subscriber = $otherSubscriber -> $Delivered")
        )
      }
    }

    "log an error when returning event back to the queue fails" in new TestCase {

      val event                   = testCategoryEvents.generateOne
      val subscriber              = subscriberUrls.generateOne
      val backToTheQueueException = exceptions.generateOne
      val otherSubscriber         = subscriberUrls.generateOne

      inSequence {
        givenThereIs(freeSubscriber = subscriber)
        givenEventFinder(returns = Some(event))
        givenSending(event, to = subscriber, got = Misdelivered)
        givenDispatchRecoveryExists(subscriber, event)
        expectRemoval(subscriber)
        expectEventReturnedToTheQueue(event = event, got = backToTheQueueException.raiseError[IO, Unit])

        givenThereIs(freeSubscriber = otherSubscriber)
        givenEventFinder(returns = Some(event))
        givenSending(event, to = otherSubscriber, got = Delivered)
        givenDispatchRecoveryExists(otherSubscriber, event)
        expectSendingRegistered(event, otherSubscriber)

        givenNoMoreEvents()
      }

      distributor.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"$categoryName: $event, subscriber = $subscriber -> $Misdelivered"),
          Error(s"$categoryName: $event -> returning an event to the queue failed", backToTheQueueException),
          Info(s"$categoryName: $event, subscriber = $otherSubscriber -> $Delivered")
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
    val eventDelivery            = mock[EventDelivery[IO, TestCategoryEvent]]
    private val dispatchRecovery = mock[DispatchRecovery[IO, TestCategoryEvent]]
    val logger                   = TestLogger[IO]()
    val distributor = new EventsDistributorImpl[IO, TestCategoryEvent](
      categoryName,
      subscribers,
      eventsFinder,
      eventsSender,
      eventDelivery,
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

    def expectEventReturnedToTheQueue(event: TestCategoryEvent, got: IO[Unit]) =
      (dispatchRecovery.returnToQueue _)
        .expects(event)
        .returning(got)

    def givenNoMoreEvents() =
      (eventsFinder.popEvent _)
        .expects()
        .returning(Option.empty[TestCategoryEvent].pure[IO])
        .anyNumberOfTimes()

    def givenThereIs(freeSubscriber: SubscriberUrl) = {
      def onCallFunction(f: SubscriberUrl => IO[Unit]): IO[Unit] = f(freeSubscriber)
      (subscribers.runOnSubscriber _).expects(*).onCall(onCallFunction _)
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

    def expectSendingRegistered(event: TestCategoryEvent, to: SubscriberUrl): Any =
      (eventDelivery.registerSending _)
        .expects(event, to)
        .returning(().pure[IO])
  }
}
