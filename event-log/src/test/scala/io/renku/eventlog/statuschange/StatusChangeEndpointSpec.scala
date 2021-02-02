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
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventStatuses}
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.http.InfoMessage._
import ch.datascience.http.server.EndpointTester._
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.metrics.LabeledGauge
import doobie.free.connection.ConnectionIO
import eu.timepit.refined.api.Refined
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.prometheus.client.Gauge
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.statuschange.commands.UpdateResult.Updated
import io.renku.eventlog.statuschange.commands._
import io.renku.eventlog.{EventLogDB, EventProcessingTime}
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

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

        val request = Request[IO]()

        val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

        response.status                          shouldBe Ok
        response.contentType                     shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event status updated")

        logger.expectNoLogs()
      }

    "decode payload from the body, " +
      "perform status update " +
      s"and return $Conflict if no event gets updated - status case" in new TestCase {

        val eventId = command.eventId

        (commandsRunner.run _)
          .expects(command)
          .returning(UpdateResult.Conflict.pure[IO])

        val request = Request[IO]()

        val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

        response.status                          shouldBe Conflict
        response.contentType                     shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event status cannot be updated")

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

        val request = Request[IO]()

        val response = changeStatusWithSuccessfulDecode(command)(eventId, request).unsafeRunSync()

        response.status                          shouldBe InternalServerError
        response.contentType                     shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage(errorMessage.value)

        logger.loggedOnly(Error(errorMessage.value))
      }

    s"return $BadRequest if decoding payload fails" in new TestCase {

      val eventId = compoundEventIds.generateOne

      val request = Request[IO]()

      val response = changeStatusWithFailingDecode()(eventId, request).unsafeRunSync()

      response.status      shouldBe BadRequest
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage(
        s"Invalid event"
      )

      logger.expectNoLogs()
    }

    s"return $InternalServerError when updating event status fails" in new TestCase {

      val eventId = command.eventId

      val exception = exceptions.generateOne
      (commandsRunner.run _)
        .expects(command)
        .returning(exception.raiseError[IO, UpdateResult])

      val request = Request[IO]()

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
        Set(Kleisli(_ => command.some.pure[IO])),
        logger
      ).changeStatus _

    def changeStatusWithFailingDecode() =
      new StatusChangeEndpoint[IO](
        commandsRunner,
        Set(Kleisli(_ => Option.empty[ChangeStatusCommand[IO]].pure[IO])),
        logger
      ).changeStatus _

    private case class MockChangeStatusCommand() extends ChangeStatusCommand[IO] {
      val eventId: CompoundEventId = compoundEventIds.generateOne
      val status:  EventStatus     = eventStatuses.generateOne
      val queries: NonEmptyList[SqlQuery[Int]] =
        nonEmptyStrings().generateNonEmptyList().map(s => SqlQuery[Int](1.pure[ConnectionIO], Refined.unsafeApply(s)))
      def updateGauges(updateResult: UpdateResult)(implicit
          transactor:                DbTransactor[IO, EventLogDB]
      ): IO[Unit] = ().pure[IO]

      def maybeProcessingTime: Option[EventProcessingTime] = eventProcessingTimes.generateOption
    }

    lazy val changeStatusCommands: Gen[ChangeStatusCommand[IO]] = Gen.const(MockChangeStatusCommand())
  }

  private class GaugeStub extends LabeledGauge[IO, projects.Path] {
    override def set(labelValue: (projects.Path, Double)) = IO.unit

    override def increment(labelValue: projects.Path) = IO.unit

    override def decrement(labelValue: projects.Path) = IO.unit

    override def reset() = IO.unit

    protected override def gauge = Gauge.build("name", "help").create()
  }
}
