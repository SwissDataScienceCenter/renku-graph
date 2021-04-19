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

package io.renku.eventlog.statuschange

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventProcessingTimes, eventStatuses}
import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.http.InfoMessage._
import ch.datascience.http.server.EndpointTester._
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.metrics.LabeledGauge
import eu.timepit.refined.api.Refined
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.circe.syntax._
import io.prometheus.client.Gauge
import io.renku.eventlog.statuschange.CommandFindingResult.{CommandFound, NotSupported, PayloadMalformed}
import io.renku.eventlog.statuschange.commands.UpdateResult.Updated
import io.renku.eventlog.statuschange.commands._
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.multipart.{Multipart, Part}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import skunk.Session

class StatusChangeEndpointSpec
    extends AnyWordSpec
    with MockFactory
    with TableDrivenPropertyChecks
    with should.Matchers {

  "changeStatus" should {

    "decode payload from the body, " +
      "perform status update " +
      s"and return $Ok if all went fine status case" in new TestCase {

        val eventId = command.eventId

        (commandsRunner.run _)
          .expects(command)
          .returning(Updated.pure[IO])

        val request = createValidRequest()

        val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

        response.status                          shouldBe Ok
        response.contentType                     shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event status updated")

        logger.expectNoLogs()
      }

    "decode payload from the body, " +
      "perform status update " +
      s"and return $InternalServerError if there was an error during update - status case" in new TestCase {

        val eventId = command.eventId

        val errorMessage = nonBlankStrings().generateOne
        (commandsRunner.run _)
          .expects(command)
          .returning(UpdateResult.Failure(errorMessage).pure[IO])

        val request = createValidRequest()

        val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

        response.status                          shouldBe InternalServerError
        response.contentType                     shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage(errorMessage.value)
      }

    s"return $BadRequest if all the commands return NotSupported" in new TestCase {

      val eventId = compoundEventIds.generateOne

      val request = createValidRequest()

      val response = changeStatusWithFailingDecode()(eventId, request).unsafeRunSync()

      response.status                          shouldBe BadRequest
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("No event command found")

      logger.expectNoLogs()
    }

    s"return $NotFound if the command does not find the event" in new TestCase {

      val eventId = command.eventId

      (commandsRunner.run _)
        .expects(command)
        .returning(UpdateResult.NotFound.pure[IO])

      val request = createValidRequest()

      val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

      response.status                          shouldBe NotFound
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event not found")
    }

    s"return $BadRequest when one of the command returns PayloadMalformed" in new TestCase {

      val eventId = command.eventId
      val request = createValidRequest()
      val message = nonEmptyStrings().generateOne

      val response = changeStatusWithPayloadMalformed(message)(eventId, request).unsafeRunSync()

      response.status                   shouldBe BadRequest
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(message).asJson
    }

    s"return $BadRequest when request is not a multipart request" in new TestCase {

      val eventId = command.eventId
      val request = Request[IO]()

      val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

      response.status                   shouldBe BadRequest
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage("Malformed event or payload").asJson
    }

    s"return $BadRequest when there's no event part" in new TestCase {

      val eventId   = command.eventId
      val multipart = Multipart[IO](Vector())
      val request   = Request[IO]().withEntity(multipart).withHeaders(multipart.headers)

      val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

      response.status                   shouldBe BadRequest
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage("Malformed event or payload").asJson
    }

    s"return $BadRequest when the request event part is not json" in new TestCase {

      val eventId = command.eventId
      val multipart = Multipart[IO](
        Vector(
          Part.formData[IO]("event", nonEmptyStrings().generateOne, `Content-Type`(MediaType.application.json))
        )
      )
      val request = Request[IO]().withEntity(multipart).withHeaders(multipart.headers)

      val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

      response.status                   shouldBe BadRequest
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage("Malformed event or payload").asJson
    }

    s"return $BadRequest when the request does not contain a status" in new TestCase {

      val eventId = command.eventId
      val multipart = Multipart[IO](
        Vector(
          Part.formData[IO]("event", jsons.generateOne.noSpaces, `Content-Type`(MediaType.application.json))
        )
      )
      val request = Request[IO]().withEntity(multipart).withHeaders(multipart.headers)

      val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

      response.status                   shouldBe BadRequest
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage("Malformed event or payload").asJson
    }

    s"return $BadRequest when the request contains an unknown status" in new TestCase {

      val eventId = command.eventId
      val multipart = Multipart[IO](
        Vector(
          Part.formData[IO]("event",
                            json"""{"event": ${Gen.oneOf(Gen.const(""), nonEmptyStrings()).generateOne} }""".noSpaces,
                            `Content-Type`(MediaType.application.json)
          )
        )
      )
      val request = Request[IO]().withEntity(multipart).withHeaders(multipart.headers)

      val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

      response.status                   shouldBe BadRequest
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage("Malformed event or payload").asJson
    }

    s"return $BadRequest when the request contains an wrongly formatted processingTime" in new TestCase {

      val eventId = command.eventId
      val multipart = Multipart[IO](
        Vector(
          Part.formData[IO](
            "event",
            json"""{"event": ${eventStatuses.generateOne.value}, "processingTime": ${Gen
              .oneOf(Gen.const(""), nonEmptyStrings())
              .generateOne} }""".noSpaces,
            `Content-Type`(MediaType.application.json)
          )
        )
      )
      val request = Request[IO]().withEntity(multipart).withHeaders(multipart.headers)

      val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

      response.status                   shouldBe BadRequest
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage("Malformed event or payload").asJson
    }

    s"return $BadRequest when the request contains an empty message" in new TestCase {

      val eventId = command.eventId
      val multipart = Multipart[IO](
        Vector(
          Part.formData[IO](
            "event",
            json"""{"event": ${eventStatuses.generateOne.value}, "processingTime": ${eventProcessingTimes.generateOne}, "message": "" }""".noSpaces,
            `Content-Type`(MediaType.application.json)
          )
        )
      )
      val request = Request[IO]().withEntity(multipart).withHeaders(multipart.headers)

      val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

      response.status                   shouldBe BadRequest
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage("Malformed event or payload").asJson
    }

    s"return $InternalServerError when updating event status fails" in new TestCase {

      val eventId = command.eventId

      val exception = exceptions.generateOne
      (commandsRunner.run _)
        .expects(command)
        .returning(exception.raiseError[IO, UpdateResult])

      val request = createValidRequest()

      val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage("Event status update failed").asJson

      logger.loggedOnly(Error("Event status update failed", exception))
    }
  }

  private trait TestCase {
    val commandsRunner = mock[StatusUpdatesRunner[IO]]
    val logger         = TestLogger[IO]()
    val command        = changeStatusCommands.generateOne

    def changeStatusWithSuccessfulDecode(command: ChangeStatusCommand[IO]) =
      new StatusChangeEndpoint[IO](
        commandsRunner,
        Set(Kleisli(_ => (CommandFound(command): CommandFindingResult).pure[IO])),
        logger
      ).changeStatus _

    def changeStatusWithPayloadMalformed(message: String) =
      new StatusChangeEndpoint[IO](
        commandsRunner,
        Set(Kleisli(_ => (PayloadMalformed(message): CommandFindingResult).pure[IO])),
        logger
      ).changeStatus _

    def changeStatusWithFailingDecode() =
      new StatusChangeEndpoint[IO](
        commandsRunner,
        Set(Kleisli(_ => (NotSupported: CommandFindingResult).pure[IO])),
        logger
      ).changeStatus _

    private case class MockChangeStatusCommand() extends ChangeStatusCommand[IO] {
      val eventId: CompoundEventId = compoundEventIds.generateOne
      val status:  EventStatus     = eventStatuses.generateOne
      val queries: NonEmptyList[SqlQuery[IO, Int]] =
        nonEmptyStrings()
          .generateNonEmptyList()
          .map(s => SqlQuery[IO, Int](Kleisli(_ => 1.pure[IO]), Refined.unsafeApply(s)))
      def updateGauges(updateResult: UpdateResult)(implicit session: Session[IO]): IO[Unit] = ().pure[IO]

      def maybeProcessingTime: Option[EventProcessingTime] = eventProcessingTimes.generateOption
    }

    lazy val changeStatusCommands: Gen[ChangeStatusCommand[IO]] = Gen.const(MockChangeStatusCommand())

    def createValidRequest(): Request[IO] = {
      val multipart = Multipart[IO](
        Vector(
          Part.formData[IO](
            "event",
            (json"""{"status": ${eventStatuses.generateOne.value}}""" deepMerge eventProcessingTimes.generateOption
              .map(processingTime => json"""{"processingTime": ${processingTime.value.toString}}""")
              .getOrElse(Json.obj())).noSpaces,
            `Content-Type`(MediaType.application.json)
          )
        )
      )
      Request[IO]().withEntity(multipart).withHeaders(multipart.headers)
    }
  }

  private class GaugeStub extends LabeledGauge[IO, projects.Path] {
    override def set(labelValue: (projects.Path, Double)) = IO.unit

    override def increment(labelValue: projects.Path) = IO.unit

    override def decrement(labelValue: projects.Path) = IO.unit

    override def reset() = IO.unit

    protected override def gauge = Gauge.build("name", "help").create()
  }
}
