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

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class SubscriptionsSpec extends AnyWordSpec with should.Matchers {

  "add and getAll" should {

    "add the given Subscriber URL to the pool " +
      "if it hasn't been added yet" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      subscriptions.getAll.unsafeRunSync()             shouldBe List(subscriberUrl)

      val otherSubscriberUrl = subscriberUrls.generateOne
      subscriptions.add(otherSubscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      subscriptions.getAll.unsafeRunSync()                  should contain theSameElementsAs List(subscriberUrl, otherSubscriberUrl)

      logger.loggedOnly(
        Info(s"$subscriberUrl added"),
        Info(s"$otherSubscriberUrl added")
      )
    }

    "do nothing if the given Subscriber URL is already present in the pool" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      subscriptions.getAll.unsafeRunSync()             shouldBe List(subscriberUrl)

      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      subscriptions.getAll.unsafeRunSync()             shouldBe List(subscriberUrl)

      logger.loggedOnly(Info(s"$subscriberUrl added"))
    }
  }

  "remove" should {

    "remove the given Subscriber URL if present in the pool" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      subscriptions.getAll.unsafeRunSync()             shouldBe List(subscriberUrl)

      subscriptions.remove(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(
        Info(s"$subscriberUrl added"),
        Info(s"$subscriberUrl removed")
      )
    }

    "do nothing when given Subscriber URL does not exist in the pool" in new TestCase {
      subscriptions.remove(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
    }
  }

  "nextFree" should {

    "return None if there are no URLs" in new TestCase {
      subscriptions.nextFree.unsafeRunSync() shouldBe None
    }

    "return the only URL when the method called several times" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.nextFree.unsafeRunSync() shouldBe subscriberUrl.some
      subscriptions.nextFree.unsafeRunSync() shouldBe subscriberUrl.some
    }

    "return the only URL after the sleep time passes if the URL was marked as Busy" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.nextFree.unsafeRunSync() shouldBe subscriberUrl.some

      subscriptions.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      subscriptions.nextFree.unsafeRunSync()                shouldBe None

      sleep(busySleep.toMillis + 50)

      subscriptions.nextFree.unsafeRunSync() shouldBe subscriberUrl.some
    }

    "iterate over all the added urls" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      val otherUrl = subscriberUrls.generateOne
      subscriptions.add(otherUrl).unsafeRunSync() shouldBe ((): Unit)

      val receivedUrls = List(
        subscriptions.nextFree.unsafeRunSync(),
        subscriptions.nextFree.unsafeRunSync(),
        subscriptions.nextFree.unsafeRunSync()
      ).flatten
      receivedUrls should have size 3
      val first = receivedUrls.head
      receivedUrls.last      shouldBe first
      receivedUrls.tail.head should not be first
    }

    "skip over removed item even if it suppose to be the next one" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      val otherUrl = subscriberUrls.generateOne
      subscriptions.add(otherUrl).unsafeRunSync() shouldBe ((): Unit)

      val Some(first) = subscriptions.nextFree.unsafeRunSync()

      val expectedNext = if (first == subscriberUrl) otherUrl else subscriberUrl
      (subscriptions remove expectedNext).unsafeRunSync()

      subscriptions.nextFree.unsafeRunSync() shouldBe Some(first)
    }

    "skip over busy item even if it supposed to be the next one" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      val otherUrl = subscriberUrls.generateOne
      subscriptions.add(otherUrl).unsafeRunSync() shouldBe ((): Unit)

      val subscribers = Set(subscriberUrl, otherUrl)

      val busySubscriber = Gen.oneOf(subscriberUrl, otherUrl).generateOne
      subscriptions.markBusy(busySubscriber).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.nextFree.unsafeRunSync() shouldBe (subscribers - busySubscriber).headOption
      subscriptions.nextFree.unsafeRunSync() shouldBe (subscribers - busySubscriber).headOption

      sleep(busySleep.toMillis + 50)

      subscriptions.nextFree.unsafeRunSync() shouldBe Some(busySubscriber)
    }
  }

  "isNext" should {

    "return false if there are no URLs" in new TestCase {
      subscriptions.isNext.unsafeRunSync() shouldBe false
    }

    "return false if the only URL is marked busy" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.isNext.unsafeRunSync() shouldBe true

      subscriptions.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.isNext.unsafeRunSync() shouldBe false

      sleep(busySleep.toMillis + 50)

      subscriptions.isNext.unsafeRunSync() shouldBe true
    }

    "return true if there's at least a single URL" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.isNext.unsafeRunSync() shouldBe true
      subscriptions.isNext.unsafeRunSync() shouldBe true

      subscriptions.add(subscriberUrls.generateOne).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.isNext.unsafeRunSync() shouldBe true
    }
  }

  "hasOtherThan" should {

    "return false if there are no URLs" in new TestCase {
      subscriptions.hasOtherThan(subscriberUrl).unsafeRunSync() shouldBe false
    }

    "return false if there is only one URL the same as given" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.hasOtherThan(subscriberUrl).unsafeRunSync() shouldBe false
    }

    "return true if there are other URLs then the given one" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync()              shouldBe ((): Unit)
      subscriptions.add(subscriberUrls.generateOne).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.hasOtherThan(subscriberUrl).unsafeRunSync() shouldBe true
    }

    "return false if there are other URLs but they are marked busy" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      val otherSubscriber = subscriberUrls.generateOne
      subscriptions.add(otherSubscriber).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.hasOtherThan(subscriberUrl).unsafeRunSync() shouldBe true

      subscriptions.markBusy(otherSubscriber).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.hasOtherThan(subscriberUrl).unsafeRunSync() shouldBe false

      sleep(busySleep.toMillis + 50)

      subscriptions.hasOtherThan(subscriberUrl).unsafeRunSync() shouldBe true
    }
  }

  "markBusy" should {

    "succeed if there is subscriber with the given url" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.nextFree.unsafeRunSync() shouldBe None

      sleep(busySleep.toMillis + 50)

      subscriptions.nextFree.unsafeRunSync() shouldBe subscriberUrl.some
    }

    "succeed if there is no subscriber with the given url" in new TestCase {
      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.markBusy(subscriberUrls.generateOne).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.nextFree.unsafeRunSync() shouldBe subscriberUrl.some
    }
  }

  private trait TestCase {
    val subscriberUrl = subscriberUrls.generateOne
    val busySleep     = 500 millis

    private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
    private implicit val timer: Timer[IO]        = IO.timer(global)
    val logger        = TestLogger[IO]()
    val subscriptions = Subscriptions(logger, busySleep).unsafeRunSync()
  }
}
