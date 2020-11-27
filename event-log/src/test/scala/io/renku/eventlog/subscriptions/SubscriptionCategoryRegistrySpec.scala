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

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import io.circe.Json
import io.renku.eventlog.subscriptions.Generators.subscriberUrls
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Random, Success, Try}

private class SubscriptionCategoryRegistrySpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {
    "return unit when all of the categories return Unit" in new TestCase {
      categories.foreach { category =>
        (category.run _)
          .expects()
          .returning(().pure[Try])
      }

      registry.run() shouldBe Success(())
    }

    "throw an error when one of the categories throws an error" in new TestCase {
      val exception = exceptions.generateOne
      (categories.head.run _)
        .expects()
        .returning(exception.raiseError[Try, Unit])

      registry.run() shouldBe Failure(exception)
    }
  }

  "register" should {
    "return Right when at least one of the categories returns Some" in new TestCase {
      val shuffledCategories = Random.shuffle(categories.toList)
      (shuffledCategories.head.register _)
        .expects(payload)
        .returning(subscriptionCategoryPayloads.generateSome.pure[Try])

      shuffledCategories.tail.foreach { category =>
        (category.register _)
          .expects(payload)
          .returning(None.pure[Try])
      }

      registry.register(payload) shouldBe Success(Right(()))
    }

    "return Left when all of the components return None" in new TestCase {
      categories.foreach { category =>
        (category.register _)
          .expects(payload)
          .returning(None.pure[Try])
      }
      val Success(Left(error)) = registry.register(payload)
      error shouldBe a[RequestError]
    }

    "fail when one of the categories fails" in new TestCase {
      val exception = exceptions.generateOne
      (categories.head.register _)
        .expects(payload)
        .returning(exception.raiseError[Try, Option[SubscriptionCategory[Try]#PayloadType]])

      registry.register(payload) shouldBe Failure(exception)
    }
  }

  trait TestCase {

    val subscriptionCategoryPayloads: Gen[SubscriptionCategoryPayload] = for {
      url <- subscriberUrls
    } yield new SubscriptionCategoryPayload {
      override def subscriberUrl: SubscriberUrl = url
    }

    val categories: Set[SubscriptionCategory[Try]] = Gen
      .nonEmptyListOf(
        Gen.const(mock[SubscriptionCategory[Try]])
      )
      .generateOne
      .toSet

    val payload: Json = jsons.generateOne
    val registry = new SubscriptionCategoryRegistryImpl[Try](categories)
  }
}
