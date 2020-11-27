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

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import io.circe.Json
import io.renku.eventlog.subscriptions.Generators.subscriptionCategoryPayloads
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

private class SubscriptionCategoryRegistrySpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {
    "return unit when all of the categories return Unit" in new TestCase {
      val categories = noRegistrationCategories
      val registry   = new SubscriptionCategoryRegistryImpl[IO](categories)
      registry.run().unsafeRunSync() shouldBe ()
    }

    "throw an error when one of the categories throws an error" in new TestCase {
      val exception  = exceptions.generateOne
      val categories = generateCategories(withError = exception)
      println(categories)
      val registry = new SubscriptionCategoryRegistryImpl[IO](categories)
      intercept[Exception] {
        registry.run().unsafeRunSync()
      } shouldBe exception
    }
  }

  "register" should {
    "return Right when at least one of the categories returns Some" in new TestCase {
      val categories = generateCategoriesWithOneRegistration()
      val registry   = new SubscriptionCategoryRegistryImpl[IO](categories)
      registry.register(payload).unsafeRunSync() shouldBe Right(())
    }

    "return Left there are no categories" in new TestCase {
      val registry    = new SubscriptionCategoryRegistryImpl[IO](Set.empty)
      val Left(error) = registry.register(payload).unsafeRunSync()
      error shouldBe a[RequestError]
    }

    "return Left when all of the components return None" in new TestCase {
      val categories  = noRegistrationCategories
      val registry    = new SubscriptionCategoryRegistryImpl[IO](categories)
      val Left(error) = registry.register(payload).unsafeRunSync()
      error shouldBe a[RequestError]
    }

    "fail when one of the categories fails" in new TestCase {
      val exception  = exceptions.generateOne
      val categories = generateCategories(withError = exception)
      val registry   = new SubscriptionCategoryRegistryImpl[IO](categories)
      intercept[Exception] {
        registry.register(payload).unsafeRunSync()
      } shouldBe exception
    }
  }

  trait TestCase {

    private case class SuccessSubscriptionCategory() extends SubscriptionCategory[IO] {
      protected override type PayloadType = SubscriptionCategoryPayload

      override def run(): IO[Unit] = ().pure[IO]

      override def register(payload: Json): IO[Option[SubscriptionCategoryPayload]] =
        subscriptionCategoryPayloads.generateOne.some.pure[IO]

    }

    private case class NoRegistrationSubscriptionCategory() extends SubscriptionCategory[IO] {
      protected override type PayloadType = SubscriptionCategoryPayload

      override def run(): IO[Unit] = ().pure[IO]

      override def register(payload: Json): IO[Option[SubscriptionCategoryPayload]] =
        none.pure[IO]

    }

    private case class ErrorSubscriptionCategory(exception: Exception) extends SubscriptionCategory[IO] {
      protected override type PayloadType = SubscriptionCategoryPayload

      override def run(): IO[Unit] = exception.raiseError[IO, Unit]

      override def register(payload: Json): IO[Option[SubscriptionCategoryPayload]] =
        exception.raiseError[IO, Option[SubscriptionCategoryPayload]]

    }

    val noRegistrationCategories = Set[SubscriptionCategory[IO]](NoRegistrationSubscriptionCategory())

    def generateCategories(withError: Exception): Set[SubscriptionCategory[IO]] =
      Random.shuffle(
        Set[SubscriptionCategory[IO]](ErrorSubscriptionCategory(withError), NoRegistrationSubscriptionCategory())
      )

    def generateCategoriesWithOneRegistration(): Set[SubscriptionCategory[IO]] =
      Random.shuffle(Set[SubscriptionCategory[IO]](NoRegistrationSubscriptionCategory(), SuccessSubscriptionCategory()))

    val payload: Json = jsons.generateOne

  }
}
