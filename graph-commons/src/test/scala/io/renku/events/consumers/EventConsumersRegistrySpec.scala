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

package io.renku.events.consumers

import cats.effect.IO
import cats.syntax.all._
import io.renku.events.consumers.ConsumersModelGenerators._
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventConsumersRegistrySpec extends AnyWordSpec with IOSpec with should.Matchers with MockFactory {

  "run" should {

    "start all the subscriptionMechanisms" in new TestCase {

      (subscriptionMechanism0.run _)
        .expects()
        .returning(IO.unit)
      (subscriptionMechanism1.run _)
        .expects()
        .returning(IO.unit)

      registry.run().unsafeRunSync() shouldBe ()
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

      registry.renewAllSubscriptions().unsafeRunSync() shouldBe ()
    }
  }

  "handle" should {
    s"return $Accepted if one of the handlers accepts the given payload" in new TestCase {
      (handler0.tryHandling _)
        .expects(requestContent)
        .returning(EventSchedulingResult.UnsupportedEventType.pure[IO])

      (handler1.tryHandling _)
        .expects(requestContent)
        .returning(EventSchedulingResult.Accepted.pure[IO])

      registry.handle(requestContent).unsafeRunSync() shouldBe Accepted
    }

    s"return $UnsupportedEventType if none of the handlers supports the given payload" in new TestCase {

      (handler0.tryHandling _)
        .expects(requestContent)
        .returning(EventSchedulingResult.UnsupportedEventType.pure[IO])

      (handler1.tryHandling _)
        .expects(requestContent)
        .returning(EventSchedulingResult.UnsupportedEventType.pure[IO])

      registry.handle(requestContent).unsafeRunSync() shouldBe UnsupportedEventType

    }

    s"return $BadRequest if one of the handlers supports the given payload but it's malformed" in new TestCase {
      (handler0.tryHandling _)
        .expects(requestContent)
        .returning(EventSchedulingResult.BadRequest.pure[IO])

      registry.handle(requestContent).unsafeRunSync() shouldBe BadRequest
    }

    s"return $Busy if the handler returns $Busy" in new TestCase {

      (handler0.tryHandling _)
        .expects(requestContent)
        .returning(EventSchedulingResult.Busy.pure[IO])

      registry.handle(requestContent).unsafeRunSync() shouldBe Busy

    }

    s"return ${EventSchedulingResult.SchedulingError} if the handler returns ${EventSchedulingResult.SchedulingError}" in new TestCase {
      val exception = exceptions.generateOne
      (handler0.tryHandling _)
        .expects(requestContent)
        .returning(SchedulingError(exception).pure[IO])

      registry.handle(requestContent).unsafeRunSync() shouldBe SchedulingError(exception)

    }

    s"return an exception if the handler fails" in new TestCase {

      val exception: Exception = exceptions.generateOne
      (handler0.tryHandling _)
        .expects(requestContent)
        .returning(exception.raiseError[IO, EventSchedulingResult])

      intercept[Exception] {
        registry.handle(requestContent).unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
    }
  }

  private trait TestCase {
    val requestContent = eventRequestContents.generateOne

    val handler0 = mock[EventHandler[IO]]
    val handler1 = mock[EventHandler[IO]]

    val subscriptionMechanism0 = mock[SubscriptionMechanism[IO]]
    val subscriptionMechanism1 = mock[SubscriptionMechanism[IO]]

    val registry =
      new EventConsumersRegistryImpl[IO](List(handler0, handler1), List(subscriptionMechanism0, subscriptionMechanism1))
  }
}
