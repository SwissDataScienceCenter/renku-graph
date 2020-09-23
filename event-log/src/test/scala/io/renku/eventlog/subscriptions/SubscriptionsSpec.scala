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

import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import eu.timepit.refined.auto._
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class SubscriptionsSpec extends AnyWordSpec with should.Matchers with Eventually {

  "add" should {

    "make the subscriber available such that given functions can be executed" in new TestCase {

      val functionId = nonEmptyStrings().generateOne

      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      subscriptions.runOnSubscriber(function(functionId)).unsafeRunSync()

      verifyFunctionsRun(subscriberUrl -> functionId)
    }

    "wait with executing the function until there's a subscriber available" in new TestCase {

      val functionId = nonEmptyStrings().generateOne

      subscriptions.runOnSubscriber(function(functionId)).unsafeRunAsyncAndForget()

      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      verifyFunctionsRun(subscriberUrl -> functionId)
    }

    "subscribers become available once they run the function" in new TestCase {

      val function1Id = nonEmptyStrings().generateOne
      subscriptions.runOnSubscriber(function(function1Id)).unsafeRunAsyncAndForget()

      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      verifyFunctionsRun(subscriberUrl -> function1Id)

      val function2Id = nonEmptyStrings().generateOne
      subscriptions.runOnSubscriber(function(function2Id)).unsafeRunAsyncAndForget()

      verifyFunctionsRun(subscriberUrl -> function1Id, subscriberUrl -> function2Id)
    }

    "run all functions and utilise all subscribers" in new TestCase {

      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)
      val anotherSubscriberUrl = subscriberUrls.generateOne
      subscriptions.add(anotherSubscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      val functionIds = nonEmptyStrings().generateNonEmptyList(minElements = 5).toList
      functionIds
        .map(id => subscriptions.runOnSubscriber(function(id)))
        .sequence
        .unsafeRunAsyncAndForget()

      eventually {
        functionsExecutions.get
          .unsafeRunSync()
          .map { case (subscriberUrl, _) => subscriberUrl }
          .toSet shouldBe Set(subscriberUrl, anotherSubscriberUrl)
      }

      eventually {
        functionsExecutions.get.unsafeRunSync().map { case (_, functionId) => functionId } shouldBe functionIds
      }
    }
  }

  "remove" should {

    "block the function execution until there's available subscriber" in new TestCase {

      val function1Id = nonEmptyStrings().generateOne
      subscriptions.runOnSubscriber(function(function1Id)).unsafeRunAsyncAndForget()

      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      verifyFunctionsRun(subscriberUrl -> function1Id)

      subscriptions.remove(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      val function2Id = nonEmptyStrings().generateOne
      subscriptions.runOnSubscriber(function(function2Id)).unsafeRunAsyncAndForget()

      val anotherSubscriberUrl = subscriberUrls.generateOne

      subscriptions.add(anotherSubscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      verifyFunctionsRun(subscriberUrl -> function1Id, anotherSubscriberUrl -> function2Id)
    }
  }

  "markBusy" should {

    "put on hold selected subscriber" in new TestCase {

      val function1Id = nonEmptyStrings().generateOne
      subscriptions.runOnSubscriber(function(function1Id)).unsafeRunAsyncAndForget()

      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      verifyFunctionsRun(subscriberUrl -> function1Id)

      subscriptions.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      val function2Id = nonEmptyStrings().generateOne
      subscriptions.runOnSubscriber(function(function2Id)).unsafeRunAsyncAndForget()

      sleep(busySleep.toMillis + 100)

      verifyFunctionsRun(subscriberUrl -> function1Id, subscriberUrl -> function2Id)
    }

    "use some other subscriber if one is unavailable" in new TestCase {

      val function1Id = nonEmptyStrings().generateOne
      subscriptions.runOnSubscriber(function(function1Id)).unsafeRunAsyncAndForget()

      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      verifyFunctionsRun(subscriberUrl -> function1Id)

      subscriptions.markBusy(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      val anotherSubscriberUrl = subscriberUrls.generateOne
      subscriptions.add(anotherSubscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      val function2Id = nonEmptyStrings().generateOne
      subscriptions.runOnSubscriber(function(function2Id)).unsafeRunAsyncAndForget()

      verifyFunctionsRun(subscriberUrl -> function1Id, anotherSubscriberUrl -> function2Id)
    }
  }

  private trait TestCase {
    val functionsExecutions = Ref.of[IO, List[(SubscriberUrl, String)]](List.empty).unsafeRunSync()

    def function(id: String): SubscriberUrl => IO[Unit] = url => functionsExecutions.update(_ :+ (url, id))

    val subscriberUrl = subscriberUrls.generateOne
    val busySleep     = 1000 millis

    private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
    private implicit val timer: Timer[IO]        = IO.timer(global)
    val logger        = TestLogger[IO]()
    val subscriptions = Subscriptions(logger, busySleep).unsafeRunSync()

    def verifyFunctionsRun(functionIds: (SubscriberUrl, String)*) = eventually {
      functionsExecutions.get.unsafeRunSync() shouldBe functionIds
    }
  }
}
