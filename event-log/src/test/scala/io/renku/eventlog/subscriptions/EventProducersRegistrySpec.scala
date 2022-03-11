/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.renku.eventlog.subscriptions.EventProducersRegistry._
import io.renku.eventlog.subscriptions.SubscriptionCategory._
import io.renku.events.CategoryName
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

private class EventProducersRegistrySpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "run" should {

    "return unit when all of the categories return Unit" in new TestCase {
      val categories = Set[SubscriptionCategory[IO]](SubscriptionCategoryWithoutRegistration)
      val registry   = new EventProducersRegistryImpl[IO](categories)
      registry.run().unsafeRunSync() shouldBe ()
    }

    "throw an error when one of the categories throws an error" in new TestCase {
      val exception = exceptions.generateOne
      val failingRun = new FailingRun {
        override def raisedException: Exception = exception
      }
      val categories = Set[SubscriptionCategory[IO]](SubscriptionCategoryWithoutRegistration, failingRun)
      val registry   = new EventProducersRegistryImpl[IO](categories)
      intercept[Exception] {
        registry.run().unsafeRunSync()
      } shouldBe exception
    }
  }

  "register" should {

    "return SuccessfulSubscription when at least one of the categories returns Some" in new TestCase {
      val categories =
        Set[SubscriptionCategory[IO]](new SuccessfulRegistration {}, SubscriptionCategoryWithoutRegistration)
      val registry = new EventProducersRegistryImpl[IO](categories)
      registry.register(payload).unsafeRunSync() shouldBe SuccessfulSubscription
    }

    "return a request error when there are no categories" in new TestCase {
      val registry = new EventProducersRegistryImpl[IO](Set.empty)
      registry.register(payload).unsafeRunSync() shouldBe UnsupportedPayload("No category supports this payload")
    }

    "return a request error when no category can handle the payload" in new TestCase {
      val categories = Set[SubscriptionCategory[IO]](SubscriptionCategoryWithoutRegistration)
      val registry   = new EventProducersRegistryImpl[IO](categories)
      registry.register(payload).unsafeRunSync() shouldBe UnsupportedPayload("No category supports this payload")
    }

    "fail when one of the categories fails" in new TestCase {
      val exception = exceptions.generateOne
      val failingRegistration = new FailingRegistration {
        override def raisedException: Exception = exception
      }
      val categories = Set[SubscriptionCategory[IO]](failingRegistration, SubscriptionCategoryWithoutRegistration)
      val registry   = new EventProducersRegistryImpl[IO](categories)
      intercept[Exception] {
        registry.register(payload).unsafeRunSync()
      } shouldBe exception
    }
  }

  trait TestCase {
    trait TestSubscriptionCategory extends SubscriptionCategory[IO] {

      override val name: CategoryName = CategoryName(nonBlankStrings().generateOne.value)

      override def run(): IO[Unit] = ().pure[IO]

      override def register(payload: Json): IO[RegistrationResult] =
        RejectedRegistration.pure[IO]
    }

    object SubscriptionCategoryWithoutRegistration extends TestSubscriptionCategory {}

    trait RaisingException {
      def raisedException: Exception
    }

    trait FailingRun extends TestSubscriptionCategory with RaisingException {
      override def run(): IO[Unit] = raisedException.raiseError[IO, Unit]
    }

    trait SuccessfulRegistration extends TestSubscriptionCategory {
      override def register(payload: Json): IO[RegistrationResult] =
        AcceptedRegistration.pure[IO]
    }

    trait FailingRegistration extends TestSubscriptionCategory with RaisingException {
      override def register(payload: Json): IO[RegistrationResult] =
        raisedException.raiseError[IO, RegistrationResult]
    }

    val payload: Json = jsons.generateOne
  }
}
