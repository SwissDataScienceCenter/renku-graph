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
import cats.implicits._
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
import io.renku.eventlog.statuschange.commands.{ToNonRecoverableFailure, UpdateResult}
import io.renku.eventlog.statuschange.commands.UpdateResult.Updated
import io.renku.eventlog.subscriptions.EventsSender.SendingResult
import io.renku.eventlog.subscriptions.EventsSender.SendingResult.{Delivered, Misdelivered, ServiceBusy}
import io.renku.eventlog.{Event, EventMessage}
import org.scalamock.matchers.ArgCapture.CaptureAll
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class EventsDispatcherSpec extends WordSpec with MockFactory with Eventually {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout  = scaled(Span(5, Seconds)),
    interval = scaled(Span(150, Millis))
  )

  "run" should {

    "dispatch found event to a random subscriber" in new TestCase {

      (subscriptions.getAll _).expects().returning(urls.pure[IO]).atLeastOnce()

      val event = givenEvent(got = Delivered)

      givenNoMoreEvents()

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        usedUrls.values should contain oneOf (url1, url2)
      }
      logger.loggedOnly(Info(s"Event ${event.compoundEventId}, url = ${usedUrls.value} -> $Delivered"))
    }

    "dispatch found event to another random subscriber " +
      s"if delivery to the first one resulted in $ServiceBusy" in new TestCase {

      (subscriptions.getAll _).expects().returning(urls.pure[IO]).atLeastOnce()

      val event = givenEvent(got = ServiceBusy)
      (eventsSender.sendEvent _)
        .expects(capture(usedUrls), event.compoundEventId, event.body)
        .returning(Delivered.pure[IO])

      givenNoMoreEvents()

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        usedUrls.values should contain theSameElementsAs urls
      }

      val usedUrl1 +: usedUrl2 +: Nil = usedUrls.values
      logger.loggedOnly(
        Info(s"Event ${event.compoundEventId}, url = $usedUrl1 -> $ServiceBusy"),
        Info(s"Event ${event.compoundEventId}, url = $usedUrl2 -> $Delivered")
      )
    }

    s"remove subscriber which returned $Misdelivered on event dispatching " +
      "and use another subscriber to send the event" in new TestCase {

      (subscriptions.getAll _).expects().returning(urls.pure[IO]).atLeastOnce()

      val removedUrls = CaptureAll[SubscriptionUrl]()
      val event       = givenEvent(got = Misdelivered, removedUrls = removedUrls)
      (eventsSender.sendEvent _)
        .expects(capture(usedUrls), event.compoundEventId, event.body)
        .returning(Delivered.pure[IO])

      givenNoMoreEvents()

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        usedUrls.values should contain theSameElementsAs urls
      }

      val usedUrl1 +: usedUrl2 +: Nil = usedUrls.values
      eventually {
        removedUrls.values should contain only usedUrl1
      }

      logger.loggedOnly(
        Error(s"Event ${event.compoundEventId}, url = $usedUrl1 -> $Misdelivered"),
        Info(s"Event ${event.compoundEventId}, url = $usedUrl2 -> $Delivered")
      )
    }

    s"mark event with $NonRecoverableFailure status when sending it failed " +
      "and continue processing next event" in new TestCase {

      (subscriptions.getAll _).expects().returning(urls.pure[IO]).atLeastOnce()

      // failing event
      val event = events.generateOne
      (eventsFinder.popEvent _)
        .expects()
        .returning((event.compoundEventId -> event.body).some.pure[IO])

      val exception = exceptions.generateOne
      (eventsSender.sendEvent _)
        .expects(capture(usedUrls), event.compoundEventId, event.body)
        .returning(exception.raiseError[IO, SendingResult])

      val statusUpdateCommand = ToNonRecoverableFailure[IO](event.compoundEventId,
                                                            EventMessage(exception),
                                                            waitingEventsGauge,
                                                            underProcessingGauge)
      (statusUpdatesRunner.run _)
        .expects(statusUpdateCommand)
        .returning(Updated.pure[IO])

      val nextEvent = givenEvent(got = Delivered)

      givenNoMoreEvents()

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        usedUrls.values should contain atLeastOneElementOf urls
      }

      val usedUrl1 +: usedUrl2 +: Nil = usedUrls.values
      logger.loggedOnly(
        Error(s"Event ${event.compoundEventId}, url = $usedUrl1 -> $NonRecoverableFailure", exception),
        Info(s"Event ${nextEvent.compoundEventId}, url = $usedUrl2 -> $Delivered")
      )
    }

    "wait for a subscriber if there are no more to use for sending events" in new TestCase {

      (subscriptions.getAll _).expects().returning(List(url1).pure[IO])

      val event = givenEvent(got = Misdelivered)
      (subscriptions.getAll _).expects().returning(List.empty[SubscriptionUrl].pure[IO])

      (subscriptions.getAll _).expects().returning(List(url2).pure[IO]).atLeastOnce()
      givenEvent(event, got = Delivered)

      givenNoMoreEvents()

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        usedUrls.values should contain theSameElementsAs List(url1, url2)
      }

      val usedUrl1 +: usedUrl2 +: Nil = usedUrls.values
      logger.loggedOnly(
        Error(s"Event ${event.compoundEventId}, url = $usedUrl1 -> $Misdelivered"),
        Info("Waiting for subscribers"),
        Info(s"Event ${event.compoundEventId}, url = $usedUrl2 -> $Delivered")
      )
    }

    "do not fail the process if finding subscriptions fails" in new TestCase {

      val exception = exceptions.generateOne
      (subscriptions.getAll _).expects().returning(exception.raiseError[IO, List[SubscriptionUrl]])

      (subscriptions.getAll _).expects().returning(urls.pure[IO]).atLeastOnce()

      val event = givenEvent(got = Delivered)

      givenNoMoreEvents()

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        usedUrls.values should contain atLeastOneOf (url1, url2)
      }

      logger.loggedOnly(
        Error("Finding subscribers failed", exception),
        Info(s"Event ${event.compoundEventId}, url = ${usedUrls.value} -> $Delivered")
      )
    }

    "do not fail the process if finding event to process fails" in new TestCase {

      (subscriptions.getAll _).expects().returning(urls.pure[IO]).atLeastOnce()

      val exception = exceptions.generateOne
      (eventsFinder.popEvent _)
        .expects()
        .returning(exception.raiseError[IO, Option[(CompoundEventId, EventBody)]])

      val event = givenEvent(got = Delivered)

      givenNoMoreEvents()

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        usedUrls.values should contain atLeastOneOf (url1, url2)
      }

      logger.loggedOnly(
        Error("Finding events to process failed", exception),
        Info(s"Event ${event.compoundEventId}, url = ${usedUrls.value} -> $Delivered")
      )
    }

    "do not fail the process but re-dispatch the event " +
      "if removing a subscription fails" in new TestCase {

      (subscriptions.getAll _).expects().returning(urls.pure[IO]).atLeastOnce()

      val event = events.generateOne
      (eventsFinder.popEvent _)
        .expects()
        .returning((event.compoundEventId -> event.body).some.pure[IO])

      (eventsSender.sendEvent _)
        .expects(capture(usedUrls), event.compoundEventId, event.body)
        .returning(Misdelivered.pure[IO])

      val exception   = exceptions.generateOne
      val removedUrls = CaptureAll[SubscriptionUrl]()
      (subscriptions.remove _)
        .expects(capture(removedUrls))
        .returning(exception.raiseError[IO, Unit])

      (eventsSender.sendEvent _)
        .expects(capture(usedUrls), event.compoundEventId, event.body)
        .returning(Delivered.pure[IO])

      givenNoMoreEvents()

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        usedUrls.values      should contain allElementsOf urls
        usedUrls.values.head shouldBe removedUrls.value
      }

      logger.loggedOnly(
        Error("Removing subscription failed", exception),
        Error(s"Event ${event.compoundEventId}, url = ${removedUrls.value} -> $Misdelivered"),
        Info(s"Event ${event.compoundEventId}, url = ${usedUrls.value} -> $Delivered")
      )
    }

    "do not fail the process but try to mark event as failed again if doing so fails" in new TestCase {

      (subscriptions.getAll _).expects().returning(urls.pure[IO]).atLeastOnce()

      val event = events.generateOne
      (eventsFinder.popEvent _)
        .expects()
        .returning((event.compoundEventId -> event.body).some.pure[IO])

      val exception = exceptions.generateOne
      (eventsSender.sendEvent _)
        .expects(capture(usedUrls), event.compoundEventId, event.body)
        .returning(exception.raiseError[IO, SendingResult])

      val statusUpdateCommand = ToNonRecoverableFailure[IO](event.compoundEventId,
                                                            EventMessage(exception),
                                                            waitingEventsGauge,
                                                            underProcessingGauge)
      val eventStatusChangeException = exceptions.generateOne
      (statusUpdatesRunner.run _)
        .expects(statusUpdateCommand)
        .returning(eventStatusChangeException.raiseError[IO, UpdateResult])
        .twice()
      (statusUpdatesRunner.run _)
        .expects(statusUpdateCommand)
        .returning(UpdateResult.Updated.pure[IO])

      givenNoMoreEvents()

      dispatcher.run.unsafeRunAsyncAndForget()

      eventually {
        usedUrls.values should contain oneElementOf urls
      }

      eventually {
        logger.loggedOnly(
          Error(s"Marking event as $NonRecoverableFailure failed", eventStatusChangeException),
          Error(s"Marking event as $NonRecoverableFailure failed", eventStatusChangeException),
          Error(s"Event ${event.compoundEventId}, url = ${usedUrls.value} -> $NonRecoverableFailure", exception)
        )
      }
    }
  }

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  private trait TestCase {
    implicit val usedUrls: CaptureAll[SubscriptionUrl] = CaptureAll[SubscriptionUrl]()

    val urls @ url1 +: url2 +: Nil =
      subscriptionUrls.generateNonEmptyList(minElements = 2, maxElements = 2).toList

    val waitingEventsGauge   = mock[LabeledGauge[IO, projects.Path]]
    val underProcessingGauge = mock[LabeledGauge[IO, projects.Path]]
    val subscriptions        = mock[TestIOSubscriptions]
    val eventsFinder         = mock[EventFetcher[IO]]
    val statusUpdatesRunner  = mock[StatusUpdatesRunner[IO]]
    val eventsSender         = mock[EventsSender[IO]]
    val logger               = TestLogger[IO]()
    val dispatcher = new EventsDispatcher(
      subscriptions,
      eventsFinder,
      statusUpdatesRunner,
      eventsSender,
      waitingEventsGauge,
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
        event:           Event = events.generateOne,
        got:             SendingResult,
        removedUrls:     CaptureAll[SubscriptionUrl] = CaptureAll[SubscriptionUrl]()
    )(implicit usedUrls: CaptureAll[SubscriptionUrl]): Event = {

      (eventsFinder.popEvent _)
        .expects()
        .returning((event.compoundEventId -> event.body).some.pure[IO])

      (eventsSender.sendEvent _)
        .expects(capture(usedUrls), event.compoundEventId, event.body)
        .returning(got.pure[IO])

      got match {
        case Delivered   => ()
        case ServiceBusy => ()
        case Misdelivered =>
          (subscriptions.remove _)
            .expects(capture(removedUrls))
            .returning(IO.unit)
      }

      event
    }
  }
}
