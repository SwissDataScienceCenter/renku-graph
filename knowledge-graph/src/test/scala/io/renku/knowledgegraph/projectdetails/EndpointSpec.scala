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

package io.renku.knowledgegraph.projectdetails

import ProjectsGenerators._
import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.http.InfoMessage._
import io.renku.http.server.EndpointTester._
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Warn}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import model._
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EndpointSpec extends AnyWordSpec with MockFactory with ScalaCheckPropertyChecks with should.Matchers with IOSpec {

  "GET /projects/:path" should {

    "respond with OK and the found project details" in new TestCase {
      val project       = resourceProjects.generateOne
      val maybeAuthUser = authUsers.generateOption
      (projectFinder.findProject _)
        .expects(project.path, maybeAuthUser)
        .returning(project.some.pure[IO])

      val json = jsons.generateOne
      (jsonEncoder.encode _).expects(project).returns(json)

      val response = endpoint.`GET /projects/:path`(project.path, maybeAuthUser).unsafeRunSync()

      response.status                   shouldBe Ok
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe json

      logger.loggedOnly(
        Warn(s"Finding '${project.path}' details finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "respond with NOT_FOUND if there is no project with the given path" in new TestCase {

      val path          = projectPaths.generateOne
      val maybeAuthUser = authUsers.generateOption

      (projectFinder.findProject _)
        .expects(path, maybeAuthUser)
        .returning(None.pure[IO])

      val response = endpoint.`GET /projects/:path`(path, maybeAuthUser).unsafeRunSync()

      response.status      shouldBe NotFound
      response.contentType shouldBe Some(`Content-Type`(application.json))

      response.as[Json].unsafeRunSync() shouldBe InfoMessage(s"No '$path' project found").asJson

      logger.loggedOnly(
        Warn(s"Finding '$path' details finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "respond with INTERNAL_SERVER_ERROR if finding project details fails" in new TestCase {

      val path          = projectPaths.generateOne
      val maybeAuthUser = authUsers.generateOption
      val exception     = exceptions.generateOne
      (projectFinder.findProject _)
        .expects(path, maybeAuthUser)
        .returning(exception.raiseError[IO, Option[Project]])

      val response = endpoint.`GET /projects/:path`(path, maybeAuthUser).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(application.json))

      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(s"Finding '$path' project failed").asJson

      logger.loggedOnly(Error(s"Finding '$path' project failed", exception))
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val projectFinder         = mock[ProjectFinder[IO]]
    val jsonEncoder           = mock[JsonEncoder]
    val executionTimeRecorder = TestExecutionTimeRecorder[IO]()
    val endpoint              = new EndpointImpl[IO](projectFinder, jsonEncoder, executionTimeRecorder)
  }
}
