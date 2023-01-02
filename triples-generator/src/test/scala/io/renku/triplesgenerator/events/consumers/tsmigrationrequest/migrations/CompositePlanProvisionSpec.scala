/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.effect._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.eventlog.EventLogClient.EventPayload
import io.renku.graph.model.EventContentGenerators
import io.renku.graph.model.events.{EventInfo, EventStatus}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.CompositePlanProvisionSpec.TestData
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scodec.bits.ByteVector

class CompositePlanProvisionSpec extends AnyWordSpec with IOSpec with should.Matchers {

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  "loadContext" should {
    "create context when event is in an eligible status" in {
      for (status <- TestData.Status.eligible) {
        val ev = EventContentGenerators
          .eventInfos()
          .generateOne
          .copy(status = status)

        val makeContext = CompositePlanProvision.Context.create[IO](eventPayload(TestData.Payload.withCompositePlan)) _

        val ctx = makeContext(ev)
          .unsafeRunSync()
          .getOrElse(sys.error("Expected a context to be created"))

        ctx.event        shouldBe ev
        ctx.payload      shouldBe Some(TestData.Payload.withCompositePlan)
        ctx.uncompressed shouldBe "hello CompositePlan world"
      }
    }

    "create context if there is no payload" in {
      for (status <- TestData.Status.eligible) {
        val ev = EventContentGenerators
          .eventInfos()
          .generateOne
          .copy(status = status)

        val makeContext = CompositePlanProvision.Context.create[IO](noEventPayload) _

        val ctx = makeContext(ev)
          .unsafeRunSync()
          .getOrElse(sys.error("Expected a context to be created"))

        ctx.event        shouldBe ev
        ctx.payload      shouldBe None
        ctx.uncompressed shouldBe ""
      }
    }

    "create context if the payload cannot be unzipped" in {
      val ev = EventContentGenerators
        .eventInfos()
        .generateOne
        .copy(status = TestData.Status.eligible.head)

      val makeContext = CompositePlanProvision.Context.create[IO](eventPayload(TestData.Payload.invalid)) _

      val ctx = makeContext(ev)
        .unsafeRunSync()
        .getOrElse(sys.error("Expected a context to be created"))

      ctx.event        shouldBe ev
      ctx.payload      shouldBe Some(TestData.Payload.invalid)
      ctx.uncompressed shouldBe "CompositePlan"
    }

    "not create context when event status is not eligible" in {
      for (status <- TestData.Status.nonEligible) {
        val ev = EventContentGenerators
          .eventInfos()
          .generateOne
          .copy(status = status)

        val makeContext = CompositePlanProvision.Context.create[IO](eventPayload(TestData.Payload.withCompositePlan)) _

        makeContext(ev).unsafeRunSync() shouldBe None
      }
    }

    "not create context if the payload exist but not contains 'CompositePlan'" in {
      val ev = EventContentGenerators
        .eventInfos()
        .generateOne
        .copy(status = TestData.Status.eligible.head)

      val makeContext = CompositePlanProvision.Context.create[IO](eventPayload(TestData.Payload.withoutCompositePlan)) _

      makeContext(ev).unsafeRunSync() shouldBe None
    }

    "not calling getEventPayload if event has a non-eligible status" in {
      val ev = EventContentGenerators
        .eventInfos()
        .generateOne
        .copy(status = TestData.Status.nonEligible.head)

      val makeContext = CompositePlanProvision.Context.create[IO](_ => IO.raiseError(new Exception("payload called"))) _

      makeContext(ev).unsafeRunSync() shouldBe None
    }
  }

  def eventPayload(data: EventPayload): EventInfo => IO[Option[EventPayload]] =
    _ => IO.pure(Some(data))

  def noEventPayload: EventInfo => IO[Option[EventPayload]] =
    _ => IO.pure(None)
}

object CompositePlanProvisionSpec {

  object TestData {
    object Payload {
      val withoutCompositePlan = EventPayload(
        ByteVector.fromValidBase64("H4sIAPaxiGMAA8tIzcnJVyjPL8pJ4QIALTsIrwwAAAA=")
      )
      val withCompositePlan = EventPayload(
        ByteVector.fromValidBase64("H4sIAIm0iGMAA8tIzcnJV3DOzy3IL84sSQ3IScxTKM8vykkBAKlyYxoZAAAA")
      )
      val invalid = EventPayload(ByteVector.view("not gzipped".getBytes))
    }

    object Status {
      val eligible: Set[EventStatus] = Set(
        EventStatus.TriplesStore,
        EventStatus.GenerationNonRecoverableFailure,
        EventStatus.GenerationNonRecoverableFailure,
        EventStatus.TransformationNonRecoverableFailure
      )
      val nonEligible = EventStatus.all.diff(eligible)
    }
  }
}
