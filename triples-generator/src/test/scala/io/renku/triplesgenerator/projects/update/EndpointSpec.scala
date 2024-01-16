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

package io.renku.triplesgenerator.projects.update

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.syntax._
import io.renku.data.Message
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.projects
import io.renku.http.RenkuEntityCodec
import io.renku.interpreters.TestLogger
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesgenerator.api.Generators.projectUpdatesGen
import io.renku.triplesgenerator.api.ProjectUpdates
import org.http4s.MediaType.application
import org.http4s.Request
import org.http4s.Status.{BadRequest, InternalServerError, NotFound, Ok}
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class EndpointSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with AsyncMockFactory
    with RenkuEntityCodec {

  it should "decode the payload, pass it to the project updater and return Ok for Result.Updated" in {

    val slug    = projectSlugs.generateOne
    val updates = projectUpdatesGen.generateOne
    givenProjectUpdating(slug, updates, returning = ProjectUpdater.Result.Updated.pure[IO])

    endpoint.`PATCH /projects/:slug`(slug, Request[IO]().withEntity(updates.asJson)) >>= { response =>
      response.status.pure[IO].asserting(_ shouldBe Ok) >>
        response.contentType.pure[IO].asserting(_ shouldBe `Content-Type`(application.json).some) >>
        response.as[Message].asserting(_ shouldBe Message.Info("Project updated"))
    }
  }

  it should "return BadRequest if payload decoding fails" in {

    val slug    = projectSlugs.generateOne
    val request = Request[IO]().withEntity(Json.obj("visibility" -> Json.obj("newValue" -> "invalid".asJson)))

    endpoint.`PATCH /projects/:slug`(slug, request) >>= { response =>
      response.status.pure[IO].asserting(_ shouldBe BadRequest) >>
        response.contentType.pure[IO].asserting(_ shouldBe `Content-Type`(application.json).some) >>
        response.as[Message].asserting(_ shouldBe Message.Error("Invalid payload"))
    }
  }

  it should "return NotFound if project with the given slug does not exist" in {

    val slug    = projectSlugs.generateOne
    val updates = projectUpdatesGen.generateOne
    givenProjectUpdating(slug, updates, returning = ProjectUpdater.Result.NotExists.pure[IO])

    endpoint.`PATCH /projects/:slug`(slug, Request[IO]().withEntity(updates.asJson)) >>= { response =>
      response.status.pure[IO].asserting(_ shouldBe NotFound) >>
        response.contentType.pure[IO].asserting(_ shouldBe `Content-Type`(application.json).some) >>
        response.as[Message].asserting(_ shouldBe Message.Info("Project not found"))
    }
  }

  it should "return InternalServerError if project update fails" in {

    val slug      = projectSlugs.generateOne
    val updates   = projectUpdatesGen.generateOne
    val exception = exceptions.generateOne
    givenProjectUpdating(slug, updates, returning = exception.raiseError[IO, Nothing])

    endpoint.`PATCH /projects/:slug`(slug, Request[IO]().withEntity(updates.asJson)) >>= { response =>
      response.status.pure[IO].asserting(_ shouldBe InternalServerError) >>
        response.contentType.pure[IO].asserting(_ shouldBe `Content-Type`(application.json).some) >>
        response
          .as[Message]
          .asserting(_ shouldBe Message.Error.fromMessageAndStackTraceUnsafe("Project update failed", exception))
    }
  }

  private implicit val logger: TestLogger[IO] = TestLogger[IO]()
  private lazy val projectUpdater = mock[ProjectUpdater[IO]]
  private lazy val endpoint       = new EndpointImpl[IO](projectUpdater)

  private def givenProjectUpdating(slug: projects.Slug, updates: ProjectUpdates, returning: IO[ProjectUpdater.Result]) =
    (projectUpdater.updateProject _)
      .expects(slug, updates)
      .returning(returning)
}
