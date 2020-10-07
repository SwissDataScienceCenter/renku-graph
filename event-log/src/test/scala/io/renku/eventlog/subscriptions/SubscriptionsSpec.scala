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

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{ContextShift, Fiber, IO, Timer}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import eu.timepit.refined.auto._
import org.scalamock.matchers.ArgCapture.CaptureAll
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

class SubscriptionsSpec extends AnyWordSpec with MockFactory with should.Matchers with Eventually {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(3, Seconds)),
    interval = scaled(Span(150, Millis))
  )

  "add" should {

    "adds the given subscriber to the registry and logs info when it was added" in new TestCase {

      (subscribersRegistry.add _)
        .expects(subscriberUrl)
        .returning(true.pure[IO])

      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info(s"$subscriberUrl added"))
    }

    "adds the given subscriber to the registry and do not log info message when it was already added" in new TestCase {

      (subscribersRegistry.add _)
        .expects(subscriberUrl)
        .returning(false.pure[IO])

      subscriptions.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      logger.expectNoLogs()
    }
  }

  "runOnSubscriber" should {

    "run the given function when there is available subscriber" in new TestCase {

      (subscribersRegistry.findAvailableSubscriber _)
        .expects()
        .returning(subscriberUrl.some)

      val function = mockFunction[SubscriberUrl, IO[Unit]]
      function.expects(subscriberUrl).returning(IO.unit)

      subscriptions.runOnSubscriber(function).unsafeRunSync() shouldBe ((): Unit)
    }

    "block execution of the function until there's a subscriber available" in new TestCase {

      (subscribersRegistry.findAvailableSubscriber _)
        .expects()
        .returning(None)

      (subscribersRegistry.findAvailableSubscriber _)
        .expects()
        .returning(subscriberUrl.some)

      val subscribersCount = nonNegativeInts().generateOne
      (subscribersRegistry.subscriberCount _)
        .expects()
        .returning(subscribersCount)

      val function = mockFunction[SubscriberUrl, IO[Unit]]
      function.expects(subscriberUrl).returning(IO.unit)

      val hook = CaptureAll[Option[Deferred[IO, Unit]]]()
      (executionHookContainer.set _)
        .expects(capture(hook))
        .returning(IO.unit)

      subscriptions.runOnSubscriber(function).unsafeRunAsyncAndForget()

      eventually {
        hook.value.map(_.complete(()).unsafeRunSync())
      }

      logger.loggedOnly(Info(s"All $subscribersCount subscribers are busy; waiting for one to become available"))
    }
  }

  "delete" should {
    "completely remove a subscriber from the registry" in new TestCase {
      (subscribersRegistry.delete _)
        .expects(subscriberUrl)
        .returning(true.pure[IO])

      subscriptions.delete(subscriberUrl = subscriberUrl).unsafeRunSync()

      logger.loggedOnly(Info(s"$subscriberUrl gone - deleting"))
    }

    "not log if nothing was deleted" in new TestCase {
      (subscribersRegistry.delete _)
        .expects(subscriberUrl)
        .returning(false.pure[IO])

      subscriptions.delete(subscriberUrl = subscriberUrl).unsafeRunSync()

      logger.expectNoLogs()
    }
  }

  "markBusy" should {
    "put on hold selected subscriber" in new TestCase {
      (subscribersRegistry.markBusy _)
        .expects(subscriberUrl)
        .returning(IO.unit)

      subscriptions.markBusy(subscriberUrl).unsafeRunSync()

      logger.loggedOnly(Info(s"$subscriberUrl busy - putting on hold"))
    }
  }

  "releaseHook" should {

    "do nothing if there is no hook in the container" in new TestCase {
      (executionHookContainer.getAndSet _)
        .expects(None)
        .returning(None.pure[IO])

      subscriptions.releaseHook().unsafeRunSync() should be((): Unit)

    }

    "execute complete on hook" in new TestCase {
      val hook = mock[Deferred[IO, Unit]]

      (executionHookContainer.getAndSet _)
        .expects(None)
        .returning(hook.some.pure[IO])

      (hook.complete _)
        .expects(())
        .returning(IO.unit)

      subscriptions.releaseHook().unsafeRunSync() should be((): Unit)

    }

  }
  "apply" should {
    "start subscriber registry" in new TestCase {
      val notifyFunction = CaptureAll[() => IO[Unit]]()
      (subscribersRegistry.start _)
        .expects(capture(notifyFunction))
        .returning(IO.pure(mock[Fiber[IO, Unit]]))

      val newSubscriptions = Subscriptions(logger, Some(subscribersRegistry.pure[IO])).unsafeRunSync()

      notifyFunction.value shouldBe newSubscriptions.asInstanceOf[HookReleaser[IO]].releaseHook
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val subscriberUrl = subscriberUrls.generateOne

    val executionHookContainer = mock[Ref[IO, Option[Deferred[IO, Unit]]]]
    val subscribersRegistry    = mock[SubscribersRegistry]
    val logger                 = TestLogger[IO]()
    val subscriptions          = new SubscriptionsImpl(executionHookContainer, subscribersRegistry, logger)
  }

}
