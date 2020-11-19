/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.graph.model.projects
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.metrics.LabeledGauge
import io.renku.eventlog.DbEventLogGenerators._
import io.renku.eventlog.EventStatus.NonRecoverableFailure
import io.renku.eventlog.statuschange.StatusUpdatesRunner
import io.renku.eventlog.statuschange.commands.UpdateResult.Updated
import io.renku.eventlog.statuschange.commands._
import io.renku.eventlog.subscriptions.EventsSender.SendingResult
import io.renku.eventlog.subscriptions.EventsSender.SendingResult.{Delivered, Misdelivered, ServiceBusy}
import io.renku.eventlog.{Event, EventMessage}
import org.scalamock.matchers.ArgCapture.CaptureAll
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class EventsDispatcherSpec extends AnyWordSpec with MockFactory with Eventually with should.Matchers {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(5, Seconds)),
    interval = scaled(Span(150, Millis))
  )

  "run" should {

    "continue dispatching events from the queue to some free subscribers" in new TestCase {

      val event           = newEvents.generateOne
      val subscriber      = subscriberUrls.generateOne
      val otherEvent      = newEvents.generateOne
      val otherSubscriber = subscriberUrls.generateOne

      inSequence {

        givenEventLog(has = Some(event))
        givenThereIs(freeSubscriber = subscriber)
        givenSending(event, to = subscriber, got = Delivered)

        givenEventLog(has = Some(otherEvent))
        givenThereIs(freeSubscriber = otherSubscriber)
        givenSending(otherEvent, to = otherSubscriber, got = Delivered)

        givenNoMoreEvents()
      }

      dispatcher.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Info(s"Event ${event.compoundEventId}, url = $subscriber -> $Delivered"),
          Info(s"Event ${otherEvent.compoundEventId}, url = $otherSubscriber -> $Delivered")
        )
      }
    }

    "mark subscriber busy and dispatch an event to some other subscriber " +
      s"if delivery to the first one resulted in $ServiceBusy" in new TestCase {

        val event           = newEvents.generateOne
        val subscriber      = subscriberUrls.generateOne
        val otherSubscriber = subscriberUrls.generateOne

        inSequence {
          givenEventLog(has = Some(event))
          givenThereIs(freeSubscriber = subscriber)
          givenSending(event, to = subscriber, got = ServiceBusy)
          expectMarkedBusy(subscriber)

          givenThereIs(freeSubscriber = otherSubscriber)
          givenSending(event, to = otherSubscriber, got = Delivered)

          givenNoMoreEvents()
        }

        dispatcher.run().unsafeRunAsyncAndForget()

        eventually {
          logger.loggedOnly(
            Info(s"Event ${event.compoundEventId}, url = $otherSubscriber -> $Delivered")
          )
        }
      }

    s"remove subscriber which returned $Misdelivered on event dispatching " +
      "and use another subscriber if exists" in new TestCase {

        val event                = newEvents.generateOne
        val subscriber           = subscriberUrls.generateOne
        val otherSubscriber      = subscriberUrls.generateOne
        val yetAnotherSubscriber = subscriberUrls.generateOne

        inSequence {
          givenEventLog(has = Some(event))
          givenThereIs(freeSubscriber = subscriber)
          givenSending(event, to = subscriber, got = Misdelivered)
          expectRemoval(subscriber)

          givenThereIs(freeSubscriber = otherSubscriber)
          givenSending(event, to = otherSubscriber, got = Misdelivered)
          expectRemoval(otherSubscriber)

          givenThereIs(freeSubscriber = yetAnotherSubscriber)
          givenSending(event, to = yetAnotherSubscriber, got = Delivered)

          givenNoMoreEvents()
        }

        dispatcher.run().unsafeRunAsyncAndForget()

        eventually {
          logger.loggedOnly(
            Error(s"Event ${event.compoundEventId}, url = $subscriber -> $Misdelivered"),
            Error(s"Event ${event.compoundEventId}, url = $otherSubscriber -> $Misdelivered"),
            Info(s"Event ${event.compoundEventId}, url = $yetAnotherSubscriber -> $Delivered")
          )
        }
      }

    s"mark event with $NonRecoverableFailure status when sending it failed " +
      "and continue processing next event" in new TestCase {

        val subscriber   = subscriberUrls.generateOne
        val failingEvent = newEvents.generateOne
        val exception    = exceptions.generateOne
        val event        = newEvents.generateOne

        val nonRecoverableStatusUpdate = CaptureAll[ToNonRecoverableFailure[IO]]()

        inSequence {
          givenEventLog(has = Some(failingEvent))
          givenThereIs(freeSubscriber = subscriber)

          (eventsSender.sendEvent _)
            .expects(subscriber, failingEvent.compoundEventId, failingEvent.body)
            .returning(exception.raiseError[IO, SendingResult])

          (statusUpdatesRunner.run _)
            .expects(capture(nonRecoverableStatusUpdate))
            .returning(Updated.pure[IO])

          givenEventLog(has = Some(event))
          givenThereIs(freeSubscriber = subscriber)
          givenSending(event, to = subscriber, got = Delivered)

          givenNoMoreEvents()
        }

        dispatcher.run().unsafeRunAsyncAndForget()

        nonRecoverableStatusUpdate.value.eventId              shouldBe failingEvent.compoundEventId
        nonRecoverableStatusUpdate.value.underProcessingGauge shouldBe underProcessingGauge
        nonRecoverableStatusUpdate.value.maybeMessage         shouldBe EventMessage(exception)

        eventually {
          logger.loggedOnly(
            Error(s"Event ${failingEvent.compoundEventId}, url = $subscriber -> $NonRecoverableFailure", exception),
            Info(s"Event ${event.compoundEventId}, url = $subscriber -> $Delivered")
          )
        }
      }

    "continue dispatching if a dispatch attempt fails" in new TestCase {

      val exception  = exceptions.generateOne
      val event      = newEvents.generateOne
      val subscriber = subscriberUrls.generateOne

      inSequence {
        givenEventLog(has = Some(event))

        (subscriptions.runOnSubscriber _)
          .expects(*)
          .returning(exception.raiseError[IO, Unit])

        givenThereIs(freeSubscriber = subscriber)
        givenSending(event, to = subscriber, got = Delivered)

        givenNoMoreEvents()
      }

      dispatcher.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error("Dispatching an event failed", exception),
          Info(s"Event ${event.compoundEventId}, url = $subscriber -> $Delivered")
        )
      }
    }

    "continue dispatching if finding event to process fails" in new TestCase {

      val exception  = exceptions.generateOne
      val event      = newEvents.generateOne
      val subscriber = subscriberUrls.generateOne

      inSequence {
        (eventsFinder.popEvent _)
          .expects()
          .returning(exception.raiseError[IO, Option[(CompoundEventId, EventBody)]])

        (eventsFinder.popEvent _)
          .expects()
          .returning(exception.raiseError[IO, Option[(CompoundEventId, EventBody)]])

        // retry fetching some new event
        givenEventLog(has = Some(event))
        givenThereIs(freeSubscriber = subscriber)
        givenSending(event, to = subscriber, got = Delivered)

        givenNoMoreEvents()
      }

      dispatcher.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error("Finding events to dispatch failed", exception),
          Error("Finding events to dispatch failed", exception),
          Info(s"Event ${event.compoundEventId}, url = $subscriber -> $Delivered")
        )
      }
    }

    "re-dispatch the event if marking a subscription busy fails" in new TestCase {

      val event      = newEvents.generateOne
      val exception  = exceptions.generateOne
      val subscriber = subscriberUrls.generateOne

      inSequence {
        givenEventLog(has = Some(event))

        givenThereIs(freeSubscriber = subscriber)
        givenSending(event, to = subscriber, got = ServiceBusy)

        (subscriptions.markBusy _)
          .expects(subscriber)
          .returning(exception.raiseError[IO, Unit])

        givenThereIs(freeSubscriber = subscriber)
        givenSending(event, to = subscriber, got = Delivered)

        givenNoMoreEvents()
      }

      dispatcher.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Info(s"Event ${event.compoundEventId}, url = $subscriber -> $Delivered")
        )
      }
    }

    "re-dispatch the event if removing a subscription fails" in new TestCase {

      val event           = newEvents.generateOne
      val exception       = exceptions.generateOne
      val subscriber      = subscriberUrls.generateOne
      val otherSubscriber = subscriberUrls.generateOne

      inSequence {
        givenEventLog(has = Some(event))

        givenThereIs(freeSubscriber = subscriber)
        givenSending(event, to = subscriber, got = Misdelivered)

        (subscriptions.delete _)
          .expects(subscriber)
          .returning(exception.raiseError[IO, Unit])

        givenThereIs(freeSubscriber = otherSubscriber)
        givenSending(event, to = otherSubscriber, got = Delivered)

        givenNoMoreEvents()
      }

      dispatcher.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"Event ${event.compoundEventId}, url = $subscriber -> $Misdelivered"),
          Info(s"Event ${event.compoundEventId}, url = $otherSubscriber -> $Delivered")
        )
      }
    }

    "retry changing event status if status update failed initially" in new TestCase {

      val failingEvent = newEvents.generateOne
      val exception    = exceptions.generateOne
      val nextEvent    = newEvents.generateOne
      val subscriber   = subscriberUrls.generateOne

      val nonRecoverableStatusUpdate = CaptureAll[ToNonRecoverableFailure[IO]]()

      inSequence {
        givenEventLog(has = Some(failingEvent))
        givenThereIs(freeSubscriber = subscriber)

        (eventsSender.sendEvent _)
          .expects(subscriber, failingEvent.compoundEventId, failingEvent.body)
          .returning(exception.raiseError[IO, SendingResult])

        (statusUpdatesRunner.run _)
          .expects(capture(nonRecoverableStatusUpdate))
          .returning(exception.raiseError[IO, UpdateResult])

        // retrying
        (statusUpdatesRunner.run _)
          .expects(capture(nonRecoverableStatusUpdate))
          .returning(Updated.pure[IO])

        // next event
        givenEventLog(has = Some(nextEvent))
        givenThereIs(freeSubscriber = subscriber)
        givenSending(nextEvent, to = subscriber, got = Delivered)

        givenNoMoreEvents()
      }

      dispatcher.run().unsafeRunAsyncAndForget()

      nonRecoverableStatusUpdate.value.eventId              shouldBe failingEvent.compoundEventId
      nonRecoverableStatusUpdate.value.underProcessingGauge shouldBe underProcessingGauge
      nonRecoverableStatusUpdate.value.maybeMessage         shouldBe EventMessage(exception)

      eventually {
        logger.loggedOnly(
          Error(s"Marking event as $NonRecoverableFailure failed", exception),
          Error(s"Event ${failingEvent.compoundEventId}, url = $subscriber -> $NonRecoverableFailure", exception),
          Info(s"Event ${nextEvent.compoundEventId}, url = $subscriber -> $Delivered")
        )
      }
    }
  }

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  private trait TestCase {

    val underProcessingGauge = mock[LabeledGauge[IO, projects.Path]]
    val subscriptions        = mock[Subscriptions[IO]]
    val eventsFinder         = mock[EventFetcher[IO]]
    val statusUpdatesRunner  = mock[StatusUpdatesRunner[IO]]
    val eventsSender         = mock[EventsSender[IO]]
    val logger               = TestLogger[IO]()
    val dispatcher = new EventsDispatcher(
      subscriptions,
      eventsFinder,
      statusUpdatesRunner,
      eventsSender,
      underProcessingGauge,
      logger,
      noEventSleep = 250 millis,
      onErrorSleep = 250 millis
    )

    def givenNoMoreEvents() =
      (eventsFinder.popEvent _)
        .expects()
        .returning(Option.empty[(CompoundEventId, EventBody)].pure[IO])
        .anyNumberOfTimes()

    def givenThereIs(freeSubscriber: SubscriberUrl) =
      (subscriptions.runOnSubscriber _).expects(*).onCall { f: (SubscriberUrl => IO[Unit]) =>
        f(freeSubscriber)
      }

    def expectRemoval(of: SubscriberUrl) =
      (subscriptions.delete _)
        .expects(of)
        .returning(IO.unit)

    def expectMarkedBusy(subscriberUrl: SubscriberUrl) =
      (subscriptions.markBusy _)
        .expects(subscriberUrl)
        .returning(IO.unit)

    def givenEventLog(has: Option[Event]) =
      (eventsFinder.popEvent _)
        .expects()
        .returning(has.map(e => e.compoundEventId -> e.body).pure[IO])

    def givenSending(event: Event, to: SubscriberUrl, got: SendingResult): Any =
      (eventsSender.sendEvent _)
        .expects(to, event.compoundEventId, event.body)
        .returning(got.pure[IO])
  }
}
