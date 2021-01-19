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

package ch.datascience.triplesgenerator.events.subscriptions

import cats.Parallel
import cats.syntax.all._
import cats.effect.{ContextShift, IO}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.global

class SubscriptionMechanismRegistrySpec extends AnyWordSpec with should.Matchers with MockFactory {

  "apply" should {

    "return a subscriptionMechanism for the given CategoryName" in new TestCase {
      (() => subscriptionMechanism0.categoryName)
        .expects()
        .returning(categoryName0)
      (() => subscriptionMechanism1.categoryName)
        .expects()
        .returning(categoryName1)

      registry(categoryName1).unsafeRunSync() shouldBe subscriptionMechanism1
    }

    "fail if there's no subscriptionMechanism for the given CategoryName" in new TestCase {
      (() => subscriptionMechanism0.categoryName)
        .expects()
        .returning(categoryName0)
      (() => subscriptionMechanism1.categoryName)
        .expects()
        .returning(categoryName1)

      val otherCategoryName = categoryNames.generateOne
      intercept[Exception] {
        registry(otherCategoryName).unsafeRunSync()
      }.getMessage shouldBe s"No SubscriptionMechanism for $otherCategoryName"
    }
  }

  "run" should {

    "start all the subscriptionMechanisms" in new TestCase {

      (subscriptionMechanism0.run _)
        .expects()
        .returning(IO.unit)
      (subscriptionMechanism1.run _)
        .expects()
        .returning(IO.unit)

      registry.run().unsafeRunSync() shouldBe ((): Unit)
    }

    "fail if one of the subscriptionMechanisms fails to start" in new TestCase {

      val exception = exceptions.generateOne
      (subscriptionMechanism0.run _)
        .expects()
        .returning(exception.raiseError[IO, Unit])
      (subscriptionMechanism1.run _)
        .expects()
        .returning(IO.unit)

      intercept[Exception] {
        registry.run().unsafeRunSync()
      } shouldBe exception
    }
  }

  "renewAllSubscriptions" should {

    "call the renewSubscription on all the subscriptionMechanism" in new TestCase {
      (subscriptionMechanism0.renewSubscription _)
        .expects()
        .returning(IO.unit)
      (subscriptionMechanism1.renewSubscription _)
        .expects()
        .returning(IO.unit)

      registry.renewAllSubscriptions().unsafeRunSync() shouldBe ((): Unit)
    }
  }

  private implicit lazy val cs:       ContextShift[IO] = IO.contextShift(global)
  private implicit lazy val parallel: Parallel[IO]     = IO.ioParallel

  private trait TestCase {
    val categoryName0          = categoryNames.generateOne
    val subscriptionMechanism0 = mock[SubscriptionMechanism[IO]]

    val categoryName1          = categoryNames.generateOne
    val subscriptionMechanism1 = mock[SubscriptionMechanism[IO]]

    val registry = new SubscriptionMechanismRegistryImpl[IO](subscriptionMechanism0, subscriptionMechanism1)
  }
}
