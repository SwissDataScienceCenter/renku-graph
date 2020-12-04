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

package io.renku.eventlog.processingstatus

import cats.data.OptionT
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.controllers.ErrorMessage.ErrorMessage
import ch.datascience.controllers.InfoMessage.InfoMessage
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import eu.timepit.refined.api.Refined
import io.circe.Decoder
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ProcessingStatusEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "findProcessingStatus" should {

    "return OK with info about processing status of events of a project with the given id " +
      "if there are some events for that project in the Log" in new TestCase {

        val processingStatus = processingStatuses.generateOne
        (statusFinder.fetchStatus _)
          .expects(projectId)
          .returning(OptionT.some[IO](processingStatus))

        Request(Method.GET, uri"events" / "projects" / projectId.toString / "status")

        val response = findProcessingStatus(projectId).unsafeRunSync()

        response.status                               shouldBe Ok
        response.contentType                          shouldBe Some(`Content-Type`(application.json))
        response.as[ProcessingStatus].unsafeRunSync() shouldBe processingStatus

        logger.expectNoLogs()
      }

    "return NOT_FOUND if there are no events for a project with the given id" in new TestCase {

      (statusFinder.fetchStatus _)
        .expects(projectId)
        .returning(OptionT.none[IO, ProcessingStatus])

      Request(Method.GET, uri"events" / "projects" / projectId.toString / "status")

      val response = findProcessingStatus(projectId).unsafeRunSync()

      response.status                          shouldBe NotFound
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("No processing status found")

      logger.expectNoLogs()
    }

    "return INTERNAL_SERVER_ERROR when finding processing status for a project with the given id fails" in new TestCase {

      val exception = exceptions.generateOne
      (statusFinder.fetchStatus _)
        .expects(projectId)
        .returning(OptionT.liftF(exception.raiseError[IO, ProcessingStatus]))

      Request(Method.GET, uri"events" / "projects" / projectId.toString / "status")

      val response = findProcessingStatus(projectId).unsafeRunSync()

      val expectedMessage = s"Finding processing status for project $projectId failed"
      response.status                           shouldBe InternalServerError
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(expectedMessage)

      logger.loggedOnly(Error(expectedMessage, exception))
    }
  }

  private trait TestCase {
    val projectId = projectIds.generateOne

    val statusFinder         = mock[ProcessingStatusFinder[IO]]
    val logger               = TestLogger[IO]()
    val findProcessingStatus = new ProcessingStatusEndpoint[IO](statusFinder, logger).findProcessingStatus _
  }

  private implicit val processingStatusDecoder: Decoder[ProcessingStatus] = cursor =>
    for {
      done     <- cursor.downField("done").as[Int]
      total    <- cursor.downField("total").as[Int]
      progress <- cursor.downField("progress").as[Double]
    } yield ProcessingStatus(Refined.unsafeApply(done), Refined.unsafeApply(total), Refined.unsafeApply(progress))

  private implicit val entityDecoder: EntityDecoder[IO, ProcessingStatus] =
    org.http4s.circe.jsonOf[IO, ProcessingStatus]

  private implicit val processingStatuses: Gen[ProcessingStatus] = for {
    total <- positiveInts(max = Integer.MAX_VALUE)
    done  <- positiveInts(max = total.value)
  } yield ProcessingStatus.from[Try](done.value, total.value).fold(throw _, identity)
}
