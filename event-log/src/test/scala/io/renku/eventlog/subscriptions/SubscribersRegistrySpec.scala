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
import java.util.concurrent.ConcurrentHashMap

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits.GenOps
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import eu.timepit.refined.auto._
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Minutes, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class SubscribersRegistrySpec extends AnyWordSpec with MockFactory with should.Matchers with Eventually {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(5, Minutes)),
    interval = scaled(Span(100, Millis))
  )

  "add" should {

    "adds the given subscriber to the registry" in new TestCase {

      subscribersRegistry.add(subscriberUrl).unsafeRunSync()                       shouldBe true
      subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync() shouldBe subscriberUrl

      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe false
    }

    "move the given subscriber from the busy state to available" in new TestCase {

      subscribersRegistry.add(subscriberUrl).unsafeRunSync()                       shouldBe true
      subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync() shouldBe subscriberUrl

      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync()                  shouldBe ((): Unit)
      subscribersRegistry.add(subscriberUrl).unsafeRunSync()                       shouldBe true
      subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync() shouldBe subscriberUrl
    }

    "add the given subscriber if it was deleted" in new TestCase {

      subscribersRegistry.add(subscriberUrl).unsafeRunSync()                       shouldBe true
      subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync() shouldBe subscriberUrl

      subscribersRegistry.delete(subscriberUrl).unsafeRunSync()                    shouldBe true
      subscribersRegistry.add(subscriberUrl).unsafeRunSync()                       shouldBe true
      subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync() shouldBe subscriberUrl
    }
  }

  "findAvailableSubscriber" should {

    "not always return the same subscriber" in new TestCase {

      val subscribers = subscriberUrls.generateNonEmptyList(minElements = 10, maxElements = 20).toList

      subscribers.map(subscribersRegistry.add).sequence.unsafeRunSync()

      subscribersRegistry.subscriberCount() shouldBe subscribers.size

      val subscribersFound = (1 to 20).foldLeft(Set.empty[SubscriberUrl]) { (returnedSubscribers, _) =>
        returnedSubscribers + subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync()
      }
      subscribersFound.size should be > 1
    }

    "return subscribers from the available pool" in new TestCase {

      override val busySleep = 10 seconds

      val busySubscriber = subscriberUrls.generateOne
      subscribersRegistry.add(busySubscriber).unsafeRunSync()                      shouldBe true
      subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync() shouldBe busySubscriber
      subscribersRegistry.markBusy(busySubscriber).unsafeRunSync()                 shouldBe ((): Unit)

      subscribersRegistry.add(subscriberUrl).unsafeRunSync()                       shouldBe true
      subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync() shouldBe subscriberUrl

      val subscribersFound = (1 to 10).foldLeft(Set.empty[SubscriberUrl]) { (returnedSubscribers, _) =>
        returnedSubscribers + subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync()
      }
      subscribersFound shouldBe Set(subscriberUrl)
    }

    "be able to queue callers when all subscribers are busy" in new TestCase {

      val collectedCallerIds = new ConcurrentHashMap[Unit, List[Int]]()
      collectedCallerIds.put((), List.empty[Int])

      val callerIds = (1 to 5).toList

      callerIds
        .map(callerId => IO(callFindSubscriber(callerId, collectedCallerIds)))
        .sequence
        .start
        .unsafeRunAsyncAndForget()

      subscribersRegistry.add(subscriberUrl).unsafeRunSync() shouldBe true

      eventually {
        collectedCallerIds.get(()) shouldBe callerIds
      }
    }
  }

  "delete" should {

    "do nothing if the subscriber is not there" in new TestCase {
      subscribersRegistry.delete(subscriberUrl).unsafeRunSync() shouldBe false
      subscribersRegistry.subscriberCount()                     shouldBe 0
    }

    "remove the subscriber if it's busy" in new TestCase {
      subscribersRegistry.add(subscriberUrl).unsafeRunSync()                       shouldBe true
      subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync() shouldBe subscriberUrl

      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      subscribersRegistry.subscriberCount()                       shouldBe 1

      subscribersRegistry.delete(subscriberUrl).unsafeRunSync() shouldBe true
      subscribersRegistry.subscriberCount()                     shouldBe 0
    }
  }

  "markBusy" should {

    "make the subscriber temporarily unavailable" in new TestCase {

      subscribersRegistry.add(subscriberUrl).unsafeRunSync()                       shouldBe true
      subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync() shouldBe subscriberUrl

      val startTime = Instant.now()
      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      // this will block until the busy subscriber becomes available again
      subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync() shouldBe subscriberUrl
      val endTime = Instant.now()

      (endTime.toEpochMilli - startTime.toEpochMilli) should be > busySleep.toMillis
      (endTime.toEpochMilli - startTime.toEpochMilli) should be < (busySleep + checkupInterval + (200 millis)).toMillis

      eventually {
        logger.loggedOnly(
          Info(s"All 1 subscribers are busy; waiting for one to become available"),
          Info(s"$subscriberUrl taken from busy state")
        )
      }
    }

    "extend unavailable time if the subscriber is already unavailable" in new TestCase {
      subscribersRegistry.add(subscriberUrl).unsafeRunSync()                       shouldBe true
      subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync() shouldBe subscriberUrl

      val startTime = Instant.now()
      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      sleep((busySleep - (100 millis)).toMillis)

      subscribersRegistry.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      subscribersRegistry.findAvailableSubscriber().flatMap(_.get).unsafeRunSync() shouldBe subscriberUrl
      val endTime = Instant.now()

      (endTime.toEpochMilli - startTime.toEpochMilli) should be > (busySleep - (100 millis) + busySleep).toMillis
      (endTime.toEpochMilli - startTime.toEpochMilli) should be < (busySleep * 2 + checkupInterval).toMillis
    }
  }

  private trait TestCase {

    implicit val cs:    ContextShift[IO] = IO.contextShift(global)
    implicit val timer: Timer[IO]        = IO.timer(global)

    val subscriberUrl = subscriberUrls.generateOne

    val busySleep                = 500 milliseconds
    val checkupInterval          = 500 milliseconds
    val logger                   = TestLogger[IO]()
    lazy val subscribersRegistry = SubscribersRegistry(logger, checkupInterval, busySleep).unsafeRunSync()

    def callFindSubscriber(callerId: Int, collectedCallers: ConcurrentHashMap[Unit, List[Int]]) = {

      def collectCallerId(callerId: Int) =
        collectedCallers.merge((), List(callerId), (t: List[Int], u: List[Int]) => t ++ u)

      subscribersRegistry
        .findAvailableSubscriber()
        .flatMap {
          _.get.map { ref =>
            collectCallerId(callerId)
            ref
          }
        }
        .unsafeRunSync() shouldBe subscriberUrl
    }
  }
}
