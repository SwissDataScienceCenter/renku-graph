/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.projects.create

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.syntax._
import io.renku.data.Message
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesgenerator.api.Generators.newProjectsGen
import io.renku.triplesgenerator.api.NewProject
import org.http4s.MediaType.application
import org.http4s.Request
import org.http4s.Status.{BadRequest, Created, InternalServerError}
import org.http4s.circe.CirceEntityCodec._
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class EndpointSpec extends AsyncFlatSpec with CustomAsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "decode the payload, pass it to the project creator and return Ok on success" in {

    val newProject = newProjectsGen.generateOne
    givenProjectCreation(newProject, returning = ().pure[IO])

    endpoint.`POST /projects`(Request[IO]().withEntity(newProject.asJson)) >>= { response =>
      response.status.pure[IO].asserting(_ shouldBe Created) >>
        response.contentType.pure[IO].asserting(_ shouldBe `Content-Type`(application.json).some) >>
        response.as[Message].asserting(_ shouldBe Message.Info("Project created"))
    }
  }

  it should "return BadRequest if payload decoding fails" in {

    val request = Request[IO]().withEntity(Json.obj("visibility" -> Json.obj("newValue" -> "invalid".asJson)))

    endpoint.`POST /projects`(request) >>= { response =>
      response.status.pure[IO].asserting(_ shouldBe BadRequest) >>
        response.contentType.pure[IO].asserting(_ shouldBe `Content-Type`(application.json).some) >>
        response.as[Message].asserting(_ shouldBe Message.Error("Invalid payload"))
    }
  }

  it should "return InternalServerError if project creation fails" in {

    val newProject = newProjectsGen.generateOne
    val exception  = exceptions.generateOne
    givenProjectCreation(newProject, returning = exception.raiseError[IO, Nothing])

    endpoint.`POST /projects`(Request[IO]().withEntity(newProject.asJson)) >>= { response =>
      response.status.pure[IO].asserting(_ shouldBe InternalServerError) >>
        response.contentType.pure[IO].asserting(_ shouldBe `Content-Type`(application.json).some) >>
        response
          .as[Message]
          .asserting(_ shouldBe Message.Error.fromMessageAndStackTraceUnsafe("Project creation failed", exception))
    }
  }

  private implicit val logger: TestLogger[IO] = TestLogger[IO]()
  private lazy val projectCreator = mock[ProjectCreator[IO]]
  private lazy val endpoint       = new EndpointImpl[IO](projectCreator)

  private def givenProjectCreation(newProject: NewProject, returning: IO[Unit]) =
    (projectCreator.createProject _)
      .expects(newProject)
      .returning(returning)
}
