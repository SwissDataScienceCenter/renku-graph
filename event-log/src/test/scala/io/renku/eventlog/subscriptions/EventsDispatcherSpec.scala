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
import eu.timepit.refined.auto._
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
    timeout  = scaled(Span(5, Seconds)),
    interval = scaled(Span(150, Millis))
  )

  "run" should {

    "wait until there are some subscriptions" in new TestCase {

      val event = events.generateOne

      inSequence {
        isNextSubscriber(returning = false)
        isNextSubscriber(returning = true)

        findingEvent(returning = Some(event))

        nextFree(returning = url1.some)
        sending(event, got = Delivered, forUrl = url1)

        givenNoMoreEvents()
      }

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Info("Waiting for subscribers"),
          Info(s"Event ${event.compoundEventId}, url = $url1 -> $Delivered")
        )
      }
    }

    "dispatch found event to the first free subscriber" in new TestCase {

      val event = events.generateOne

      inSequence {
        isNextSubscriber(returning = true)

        findingEvent(returning = Some(event))
        nextFree(returning     = url1.some)
        sending(event, got     = Delivered, forUrl = url1)

        givenNoMoreEvents()
      }

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(Info(s"Event ${event.compoundEventId}, url = $url1 -> $Delivered"))
      }
    }

    "mark subscriber busy and dispatch found event to some other subscriber " +
      s"if delivery to the first one resulted in $ServiceBusy" in new TestCase {

      val event = events.generateOne

      inSequence {
        isNextSubscriber(returning = true)

        findingEvent(returning = Some(event))

        nextFree(returning = url1.some)
        sending(event, got = ServiceBusy, forUrl = url1)
        expectMarkedBusy(url1)

        nextFree(returning = url2.some)
        sending(event, got = Delivered, forUrl = url2)

        givenNoMoreEvents()
      }

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Info(s"Event ${event.compoundEventId}, url = $url2 -> $Delivered")
        )
      }
    }

    "do not pop a new event until the one already taken is not dispatched" in new TestCase {

      val event = events.generateOne

      inSequence {
        isNextSubscriber(returning = true)

        findingEvent(returning = Some(event))

        nextFree(returning = url1.some)
        sending(event, got = ServiceBusy, forUrl = url1)
        expectMarkedBusy(url1)

        nextFree(returning = url2.some)
        sending(event, got = ServiceBusy, forUrl = url2)
        expectMarkedBusy(url2)

        nextFree(returning = url1.some)
        sending(event, got = ServiceBusy, forUrl = url1)
        expectMarkedBusy(url1)

        nextFree(returning = url2.some)
        sending(event, got = Delivered, forUrl = url2)

        givenNoMoreEvents()
      }

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Info(s"Event ${event.compoundEventId}, url = $url2 -> $Delivered")
        )
      }
    }

    "do not pop a new event until the one already taken is not dispatched - single subscription case" in new TestCase {

      val event = events.generateOne

      inSequence {
        isNextSubscriber(returning = true)

        findingEvent(returning = Some(event))

        nextFree(returning = url1.some)
        sending(event, got = ServiceBusy, forUrl = url1)
        expectMarkedBusy(url1)

        nextFree(returning = None)

        isNextSubscriber(returning = false)

        isNextSubscriber(returning = true)
        nextFree(returning         = url1.some)
        sending(event, got         = ServiceBusy, forUrl = url1)
        expectMarkedBusy(url1)

        nextFree(returning = None)

        isNextSubscriber(returning = false)

        isNextSubscriber(returning = true)
        nextFree(returning         = url1.some)
        sending(event, got         = Delivered, forUrl = url1)

        givenNoMoreEvents()
      }

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Info("Waiting for subscribers"),
          Info("Waiting for subscribers"),
          Info(s"Event ${event.compoundEventId}, url = $url1 -> $Delivered")
        )
      }
    }

    s"remove subscriber which returned $Misdelivered on event dispatching " +
      "and use another subscriber if exists" in new TestCase {

      val event = events.generateOne

      inSequence {
        isNextSubscriber(returning = true)

        findingEvent(returning = Some(event))

        nextFree(returning = url1.some)
        sending(event, got = Misdelivered, forUrl = url1)
        expectRemoval(of   = url1)

        nextFree(returning = url2.some)
        sending(event, got = Delivered, forUrl = url2)

        givenNoMoreEvents()
      }

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"Event ${event.compoundEventId}, url = $url1 -> $Misdelivered"),
          Info(s"Event ${event.compoundEventId}, url = $url2 -> $Delivered")
        )
      }
    }

    s"mark event with $NonRecoverableFailure status when sending it failed " +
      "and continue processing next event" in new TestCase {

      val failingEvent = events.generateOne
      val exception    = exceptions.generateOne
      val nextEvent    = events.generateOne

      val capturedFailure = CaptureAll[ToNonRecoverableFailure[IO]]()

      inSequence {
        isNextSubscriber(returning = true)

        // failing event
        findingEvent(returning = Some(failingEvent))

        nextFree(returning = url1.some)

        (eventsSender.sendEvent _)
          .expects(url1, failingEvent.compoundEventId, failingEvent.body)
          .returning(exception.raiseError[IO, SendingResult])

        (statusUpdatesRunner.run _)
          .expects(capture(capturedFailure))
          .returning(Updated.pure[IO])
        // next event
        givenEvent(nextEvent, got = Delivered, forUrl = url2)

        givenNoMoreEvents()
      }

      dispatcher.run.unsafeRunAsyncAndForget()

      capturedFailure.value.eventId              shouldBe failingEvent.compoundEventId
      capturedFailure.value.underProcessingGauge shouldBe underProcessingGauge
      capturedFailure.value.maybeMessage         shouldBe EventMessage(exception)

      eventually {
        logger.loggedOnly(
          Error(s"Event ${failingEvent.compoundEventId}, url = $url1 -> $NonRecoverableFailure", exception),
          Info(s"Event ${nextEvent.compoundEventId}, url = $url2 -> $Delivered")
        )
      }
    }

    "do not fail the process if finding subscriptions fails" in new TestCase {

      val exception = exceptions.generateOne
      val event     = events.generateOne

      inSequence {
        (subscriptions.isNext _).expects().returning(exception.raiseError[IO, Boolean])

        isNextSubscriber(returning = true)

        givenEvent(event, got = Delivered, forUrl = url1)

        givenNoMoreEvents()
      }

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error("Finding subscribers failed", exception),
          Info(s"Event ${event.compoundEventId}, url = $url1 -> $Delivered")
        )
      }
    }

    "do not fail the process if finding event to process fails" in new TestCase {

      val exception = exceptions.generateOne
      val event     = events.generateOne

      inSequence {
        isNextSubscriber(returning = true)

        (eventsFinder.popEvent _)
          .expects()
          .returning(exception.raiseError[IO, Option[(CompoundEventId, EventBody)]])

        // retry fetching some new event
        givenEvent(event, got = Delivered, forUrl = url1)

        givenNoMoreEvents()
      }

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error("Finding events to process failed", exception),
          Info(s"Event ${event.compoundEventId}, url = $url1 -> $Delivered")
        )
      }
    }

    "do not fail the process but re-dispatch the event " +
      "if marking a subscription busy fails" in new TestCase {

      val event     = events.generateOne
      val exception = exceptions.generateOne

      inSequence {
        isNextSubscriber(returning = true)

        findingEvent(returning = Some(event))

        nextFree(returning = url1.some)

        sending(event, ServiceBusy, forUrl = url1)

        (subscriptions.markBusy _)
          .expects(url1)
          .returning(exception.raiseError[IO, Unit])

        nextFree(returning = url2.some)
        sending(event, got = Delivered, forUrl = url2)

        givenNoMoreEvents()
      }

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Info(s"Event ${event.compoundEventId}, url = $url2 -> $Delivered")
        )
      }
    }

    "do not fail the process but re-dispatch the event " +
      "if removing a subscription fails" in new TestCase {

      val event     = events.generateOne
      val exception = exceptions.generateOne

      inSequence {
        isNextSubscriber(returning = true)

        findingEvent(returning = Some(event))

        nextFree(returning = url1.some)

        sending(event, Misdelivered, forUrl = url1)

        (subscriptions.remove _)
          .expects(url1)
          .returning(exception.raiseError[IO, Unit])

        nextFree(returning = url2.some)
        sending(event, got = Delivered, forUrl = url2)

        givenNoMoreEvents()
      }

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"Event ${event.compoundEventId}, url = $url1 -> $Misdelivered"),
          Info(s"Event ${event.compoundEventId}, url = $url2 -> $Delivered")
        )
      }
    }

    "do not fail the process but try to mark event as failed again if doing so fails" in new TestCase {

      val failingEvent = events.generateOne
      val exception    = exceptions.generateOne
      val nextEvent    = events.generateOne

      val capturedFailure = CaptureAll[ToNonRecoverableFailure[IO]]()

      inSequence {
        isNextSubscriber(returning = true)

        findingEvent(returning = Some(failingEvent))

        nextFree(returning = url1.some)

        (eventsSender.sendEvent _)
          .expects(url1, failingEvent.compoundEventId, failingEvent.body)
          .returning(exception.raiseError[IO, SendingResult])

        (statusUpdatesRunner.run _)
          .expects(capture(capturedFailure))
          .returning(exception.raiseError[IO, UpdateResult])

        // retrying
        (statusUpdatesRunner.run _)
          .expects(capture(capturedFailure))
          .returning(Updated.pure[IO])

        // next event
        findingEvent(returning = Some(nextEvent))
        nextFree(returning     = url2.some)
        sending(nextEvent, got = Delivered, forUrl = url2)

        givenNoMoreEvents()
      }

      dispatcher.run.unsafeRunAsyncAndForget()

      capturedFailure.value.eventId              shouldBe failingEvent.compoundEventId
      capturedFailure.value.underProcessingGauge shouldBe underProcessingGauge
      capturedFailure.value.maybeMessage         shouldBe EventMessage(exception)

      eventually {
        logger.loggedOnly(
          Error(s"Marking event as $NonRecoverableFailure failed", exception),
          Error(s"Event ${failingEvent.compoundEventId}, url = $url1 -> $NonRecoverableFailure", exception),
          Info(s"Event ${nextEvent.compoundEventId}, url = $url2 -> $Delivered")
        )
      }
    }
  }

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  private trait TestCase {

    val url1 +: url2 +: Nil =
      subscriberUrls.generateNonEmptyList(minElements = 2, maxElements = 2).toList

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
      noSubscriptionSleep = 500 millis,
      noEventSleep        = 250 millis,
      onErrorSleep        = 250 millis
    )

    def givenNoMoreEvents() =
      (eventsFinder.popEvent _)
        .expects()
        .returning(Option.empty[(CompoundEventId, EventBody)].pure[IO])
        .anyNumberOfTimes()

    def givenEvent(
        event:  Event = events.generateOne,
        got:    SendingResult,
        forUrl: SubscriberUrl
    ): Event = {

      findingEvent(returning = Some(event))

      nextFree(returning = forUrl.some)

      sending(event, got, forUrl)

      got match {
        case Delivered   => ()
        case ServiceBusy => ()
        case Misdelivered =>
          (subscriptions.remove _)
            .expects(forUrl)
            .returning(IO.unit)
      }

      event
    }

    def isNextSubscriber(returning: Boolean) =
      (subscriptions.isNext _).expects().returning(returning.pure[IO])

    def nextFree(returning: Option[SubscriberUrl]) =
      (subscriptions.nextFree _)
        .expects()
        .returning(returning.pure[IO])

    def expectRemoval(of: SubscriberUrl) =
      (subscriptions.remove _)
        .expects(of)
        .returning(IO.unit)

    def expectMarkedBusy(subscriberUrl: SubscriberUrl) =
      (subscriptions.markBusy _)
        .expects(subscriberUrl)
        .returning(IO.unit)

    def findingEvent(returning: Option[Event]) =
      (eventsFinder.popEvent _)
        .expects()
        .returning(returning.map(e => e.compoundEventId -> e.body).pure[IO])

    def sending(
        event:  Event,
        got:    SendingResult,
        forUrl: SubscriberUrl
    ) =
      (eventsSender.sendEvent _)
        .expects(forUrl, event.compoundEventId, event.body)
        .returning(got.pure[IO])
  }

}
