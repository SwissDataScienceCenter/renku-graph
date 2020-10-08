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

import java.lang.Thread.sleep
import java.time.Instant

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits.GenOps
import ch.datascience.generators.Generators.exceptions
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import eu.timepit.refined.auto._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class SubscribersRegistrySpec extends AnyWordSpec with MockFactory with should.Matchers {

  "add" should {

    "adds the given subscriber to the registry and logs info when it was added" in new TestCase {

      subscribersRegistry.findAvailableSubscriber()          shouldBe None
      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe false
      subscribersRegistry.findAvailableSubscriber()          shouldBe Some(subscriberUrl)
    }

    "move the given subscriber from the busy state to available" in new TestCase {

      subscribersRegistry.findAvailableSubscriber() shouldBe None

      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.findAvailableSubscriber()          shouldBe Some(subscriberUrl)

      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      subscribersRegistry.findAvailableSubscriber()               shouldBe None

      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.findAvailableSubscriber()          shouldBe Some(subscriberUrl)
    }
  }

  "delete" should {

    "delete the subscriber if it's there" in new TestCase {

      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.findAvailableSubscriber()          shouldBe Some(subscriberUrl)

      subscribersRegistry.delete(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.findAvailableSubscriber()             shouldBe None
    }

    "do nothing if the subscriber is not there" in new TestCase {
      subscribersRegistry.findAvailableSubscriber()             shouldBe None
      subscribersRegistry.delete(subscriberUrl).unsafeRunSync() shouldBe false
      subscribersRegistry.findAvailableSubscriber()             shouldBe None
    }

    "remove the subscriber if it's busy" in new TestCase {
      subscribersRegistry.start(notifyWhenAvailable = () => IO.unit).unsafeRunAsyncAndForget()

      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.findAvailableSubscriber()          shouldBe Some(subscriberUrl)

      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      subscribersRegistry.findAvailableSubscriber()               shouldBe None

      subscribersRegistry.delete(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.findAvailableSubscriber()             shouldBe None

      sleep((busySleep + checkupInterval + (100 millis)).toMillis)

      subscribersRegistry.findAvailableSubscriber() shouldBe None
    }
  }

  "markBusy" should {

    "make the subscriber temporarily unavailable" in new TestCase {
      subscribersRegistry.start(notifyWhenAvailable = () => IO.unit).unsafeRunAsyncAndForget()

      subscribersRegistry.findAvailableSubscriber() shouldBe None

      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.findAvailableSubscriber()          shouldBe Some(subscriberUrl)

      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      subscribersRegistry.findAvailableSubscriber()               shouldBe None

      sleep((busySleep + checkupInterval + (100 millis)).toMillis)

      subscribersRegistry.findAvailableSubscriber() shouldBe Some(subscriberUrl)
    }

    "extend unavailable time if the subscriber is already unavailable" in new TestCase {
      subscribersRegistry.start(notifyWhenAvailable = () => IO.unit).unsafeRunAsyncAndForget()

      subscribersRegistry.findAvailableSubscriber() shouldBe None

      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.findAvailableSubscriber()          shouldBe Some(subscriberUrl)

      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      subscribersRegistry.findAvailableSubscriber()               shouldBe None

      sleep((busySleep - (100 millis)).toMillis)
      subscribersRegistry.findAvailableSubscriber() shouldBe None

      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      sleep((busySleep - (100 millis)).toMillis)
      subscribersRegistry.findAvailableSubscriber() shouldBe None

      sleep((checkupInterval + (100 millis)).toMillis)
      subscribersRegistry.findAvailableSubscriber() shouldBe Some(subscriberUrl)
    }
  }

  "subscriberCount" should {

    "count both busy and available subscribers" in new TestCase {
      subscribersRegistry.findAvailableSubscriber() shouldBe None

      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.findAvailableSubscriber()          shouldBe Some(subscriberUrl)
      subscribersRegistry.subscriberCount()                  shouldBe 1

      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      subscribersRegistry.subscriberCount()                       shouldBe 1

      val anotherSubscriberUrl = subscriberUrls.generateOne
      subscribersRegistry.add(anotherSubscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.findAvailableSubscriber()                 shouldBe Some(anotherSubscriberUrl)
      subscribersRegistry.subscriberCount()                         shouldBe 2
    }
  }

  "findAvailableSubscriber" should {

    "not always return the same subscriber" in new TestCase {

      val subscribers = subscriberUrls.generateNonEmptyList(minElements = 10, maxElements = 20).toList

      subscribers.map(subscribersRegistry.add).sequence.unsafeRunSync()

      subscribersRegistry.subscriberCount() shouldBe subscribers.size

      val subscribersFound = (1 to 20).foldLeft(Set.empty[SubscriberUrl]) { (returnedSubscribers, _) =>
        subscribersRegistry.findAvailableSubscriber().map(returnedSubscribers + _).getOrElse(returnedSubscribers)
      }
      subscribersFound.size should be > 1
    }

    "return subscribers from the available pool" in new TestCase {

      val busySubscriber = subscriberUrls.generateOne
      subscribersRegistry.add(busySubscriber).unsafeRunSync()      shouldBe true
      subscribersRegistry.findAvailableSubscriber()                shouldBe Some(busySubscriber)
      subscribersRegistry.markBusy(busySubscriber).unsafeRunSync() shouldBe ((): Unit)

      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.findAvailableSubscriber()          shouldBe Some(subscriberUrl)

      val subscribersFound = (1 to 10).foldLeft(Set.empty[SubscriberUrl]) { (returnedSubscribers, _) =>
        subscribersRegistry.findAvailableSubscriber().map(returnedSubscribers + _).getOrElse(returnedSubscribers)
      }
      subscribersFound shouldBe Set(subscriberUrl)
    }
  }

  "start" should {

    "execute the given notify function when a subscriber is moved back to the available pool" in new TestCase {
      val notifyFunction = mockFunction[IO[Unit]]
      subscribersRegistry.start(notifyWhenAvailable = notifyFunction).unsafeRunAsyncAndForget()

      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.findAvailableSubscriber()          shouldBe Some(subscriberUrl)

      notifyFunction.expects().returning(IO.unit)
      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      sleep((busySleep + checkupInterval + (100 millis)).toMillis)
      subscribersRegistry.findAvailableSubscriber() shouldBe Some(subscriberUrl)
    }

    "not break the process when executing the given notify function gives an error" in new TestCase {
      val notifyFunction = mockFunction[IO[Unit]]
      subscribersRegistry.start(notifyWhenAvailable = notifyFunction).unsafeRunAsyncAndForget()

      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.findAvailableSubscriber()          shouldBe Some(subscriberUrl)

      val exception = exceptions.generateOne
      notifyFunction.expects().returning(exception.raiseError[IO, Unit])
      notifyFunction.expects().returning(IO.unit)
      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      sleep((busySleep + checkupInterval + (100 millis)).toMillis)
      subscribersRegistry.findAvailableSubscriber() shouldBe Some(subscriberUrl)

      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      sleep((busySleep + checkupInterval + (100 millis)).toMillis)
      subscribersRegistry.findAvailableSubscriber() shouldBe Some(subscriberUrl)

      logger.loggedOnly(
        Error(s"Notifying about $subscriberUrl taken from busy state failed", exception),
        Info(s"$subscriberUrl taken from busy state"),
        Info(s"$subscriberUrl taken from busy state")
      )
    }
  }

  private trait TestCase {

    private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
    private implicit val timer: Timer[IO]        = IO.timer(global)

    val subscriberUrl       = subscriberUrls.generateOne
    val busySleep           = 500 milliseconds
    val checkupInterval     = 500 milliseconds
    val logger              = TestLogger[IO]()
    val subscribersRegistry = new SubscribersRegistry(busySleep, Instant.now, logger, checkupInterval)
  }
}
