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

package io.renku.knowledgegraph.projects.update

import Generators._
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.literal._
import io.circe.syntax._
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.projects
import io.renku.http.server.EndpointTester._
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph
import io.renku.testtools.CustomAsyncIOSpec
import org.http4s.{Request, Status}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class EndpointSpec extends AsyncFlatSpec with CustomAsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "decode the JSON payload, update the project and return Accepted on success" in {

    val authUser = authUsers.generateOne
    val slug     = projectSlugs.generateOne
    val updates  = projectUpdatesGen.suchThat(_.newImage.isEmpty).generateOne

    givenUpdatingProject(slug, updates, authUser, returning = ().pure[IO])

    endpoint.`PATCH /projects/:slug`(slug, Request[IO]().withEntity(updates.asJson), authUser) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe Status.Accepted) >>
        response.as[Message].asserting(_ shouldBe Message.Info("Project update accepted"))
    }
  }

  it should "decode the Multipart payload, update the project and return Accepted on success" in {

    val authUser = authUsers.generateOne
    val slug     = projectSlugs.generateOne
    val updates  = projectUpdatesGen.generateOne

    givenUpdatingProject(slug, updates, authUser, returning = ().pure[IO])

    MultipartRequestEncoder[IO].encode(updates).map(mp => Request[IO]().withEntity(mp).putHeaders(mp.headers)) >>=
      (req => endpoint.`PATCH /projects/:slug`(slug, req, authUser)) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe Status.Accepted) >>
        response.as[Message].asserting(_ shouldBe Message.Info("Project update accepted"))
    }
  }

  it should "return 400 BadRequest if payload is malformed" in {

    val authUser = authUsers.generateOne
    val slug     = projectSlugs.generateOne

    endpoint.`PATCH /projects/:slug`(slug,
                                     Request[IO]().withEntity(json"""{"visibility": "unknown"}"""),
                                     authUser
    ) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe Status.BadRequest) >>
        response.as[Message].asserting(_ shouldBe Message.Error("Invalid payload"))
    }
  }

  it should "return response with the status from the known exception" in {

    val authUser = authUsers.generateOne
    val slug     = projectSlugs.generateOne
    val updates  = projectUpdatesGen.suchThat(_.newImage.isEmpty).generateOne

    val failure = knowledgegraph.Generators.failures.generateOne
    givenUpdatingProject(slug, updates, authUser, returning = failure.raiseError[IO, Nothing])

    endpoint.`PATCH /projects/:slug`(slug, Request[IO]().withEntity(updates.asJson), authUser) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe failure.status) >>
        response.as[Message].asserting(_ shouldBe failure.message)
    }
  }

  it should "return 500 InternalServerError if updating project fails with an unknown exception" in {

    val authUser = authUsers.generateOne
    val slug     = projectSlugs.generateOne
    val updates  = projectUpdatesGen.suchThat(_.newImage.isEmpty).generateOne

    val exception = exceptions.generateOne
    givenUpdatingProject(slug, updates, authUser, returning = exception.raiseError[IO, Nothing])

    endpoint.`PATCH /projects/:slug`(slug, Request[IO]().withEntity(updates.asJson), authUser) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe Status.InternalServerError) >>
        response.as[Message].asserting(_ shouldBe Message.Error("Update failed"))
    }
  }

  private implicit val logger: TestLogger[IO] = TestLogger[IO]()
  private val projectUpdater = mock[ProjectUpdater[IO]]
  private lazy val endpoint  = new EndpointImpl[IO](projectUpdater)

  private def givenUpdatingProject(slug:      projects.Slug,
                                   updates:   ProjectUpdates,
                                   authUser:  AuthUser,
                                   returning: IO[Unit]
  ) = (projectUpdater.updateProject _)
    .expects(slug, updates, authUser)
    .returning(returning)
}
