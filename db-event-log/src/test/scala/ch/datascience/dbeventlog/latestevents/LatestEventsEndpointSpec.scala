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

package ch.datascience.dbeventlog.latestevents

import LatestEventsFinder._
import cats.MonadError
import cats.effect.IO
import cats.implicits._
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.InfoMessage._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog.{Event, EventLogDB, EventProject}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.{EventBody, EventId}
import ch.datascience.graph.model.projects
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import io.circe.syntax._
import io.circe.{Decoder, Json}
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class LatestEventsEndpointSpec extends WordSpec with MockFactory {

  "findLatestEvents" should {

    "return a list of event id, project and body found in the Log" in new TestCase {

      val idProjectBodyList = nonEmptyList(events).generateOne.toList.map(toIdProjectBody)
      (eventsFinder.findAllLatestEvents _)
        .expects()
        .returning(idProjectBodyList.pure[IO])

      val request = Request(Method.GET, uri"events/latest")

      val response = findLatestEvents().unsafeRunSync()

      response.status                                shouldBe Ok
      response.contentType                           shouldBe Some(`Content-Type`(application.json))
      response.as[List[IdProjectBody]].unsafeRunSync shouldBe idProjectBodyList

      logger.expectNoLogs()
    }

    "return an empty list if nothing found in the Log" in new TestCase {

      (eventsFinder.findAllLatestEvents _)
        .expects()
        .returning(List.empty[IdProjectBody].pure[IO])

      val request = Request(Method.GET, uri"events/latest")

      val response = findLatestEvents().unsafeRunSync()

      response.status                                shouldBe Ok
      response.contentType                           shouldBe Some(`Content-Type`(application.json))
      response.as[List[IdProjectBody]].unsafeRunSync shouldBe Nil

      logger.expectNoLogs()
    }

    "return INTERNAL_SERVER_ERROR when find latest events in the Log fails" in new TestCase {

      val exception = exceptions.generateOne
      (eventsFinder.findAllLatestEvents _)
        .expects()
        .returning(exception.raiseError[IO, List[IdProjectBody]])

      val request = Request(Method.GET, uri"events/latest")

      val response = findLatestEvents().unsafeRunSync()

      response.status                 shouldBe InternalServerError
      response.contentType            shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync shouldBe ErrorMessage("Finding all projects latest events failed").asJson

      logger.loggedOnly(Error("Finding all projects latest events failed", exception))
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val eventsFinder     = mock[TestLatestEventsFinder]
    val logger           = TestLogger[IO]()
    val findLatestEvents = new LatestEventsEndpoint[IO](eventsFinder, logger).findLatestEvents _
  }

  class TestLatestEventsFinder(transactor: DbTransactor[IO, EventLogDB]) extends LatestEventsFinder(transactor)

  private val toIdProjectBody: Event => IdProjectBody =
    event => (event.id, event.project, event.body)

  private implicit val idProjectBodyDecoder: Decoder[IdProjectBody] = cursor => {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    for {
      id          <- cursor.downField("id").as[EventId]
      projectId   <- cursor.downField("project").downField("id").as[projects.Id]
      projectPath <- cursor.downField("project").downField("path").as[projects.Path]
      body        <- cursor.downField("body").as[EventBody]
    } yield (id, EventProject(projectId, projectPath), body)
  }

  implicit def entityDecoder[E](implicit decoder: Decoder[E]): EntityDecoder[IO, E] = org.http4s.circe.jsonOf[IO, E]
}
