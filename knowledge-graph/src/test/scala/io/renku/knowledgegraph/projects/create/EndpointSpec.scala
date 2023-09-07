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

package io.renku.knowledgegraph.projects.create

import Generators.newProjects
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph
import io.renku.testtools.CustomAsyncIOSpec
import org.http4s.{Request, Status}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class EndpointSpec extends AsyncFlatSpec with CustomAsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "decode the Multipart payload, create the project and return Accepted on success" in {

    val authUser   = authUsers.generateOne
    val newProject = newProjects.generateOne

    givenCreatingProject(newProject, authUser, returning = ().pure[IO])

    MultipartRequestEncoder[IO].encode(newProject).map(mp => Request[IO]().withEntity(mp).putHeaders(mp.headers)) >>=
      (req => endpoint.`POST /projects`(req, authUser)) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe Status.Accepted) >>
        response.as[Message].asserting(_ shouldBe Message.Info("Project creation accepted"))
    }
  }

  it should "return 400 BadRequest if payload is malformed" in {

    val authUser = authUsers.generateOne

    endpoint.`POST /projects`(Request[IO](), authUser) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe Status.BadRequest) >>
        response.as[Message].asserting(_ shouldBe Message.Error("Invalid payload"))
    }
  }

  it should "return response with the status from the known exception" in {

    val authUser   = authUsers.generateOne
    val newProject = newProjects.generateOne

    val failure = knowledgegraph.Generators.failures.generateOne
    givenCreatingProject(newProject, authUser, returning = failure.raiseError[IO, Nothing])

    MultipartRequestEncoder[IO].encode(newProject).map(mp => Request[IO]().withEntity(mp).putHeaders(mp.headers)) >>=
      (req => endpoint.`POST /projects`(req, authUser)) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe failure.status) >>
        response.as[Message].asserting(_ shouldBe failure.message)
    }
  }

  it should "return 500 InternalServerError if creating project fails with an unknown exception" in {

    val authUser   = authUsers.generateOne
    val newProject = newProjects.generateOne

    val exception = exceptions.generateOne
    givenCreatingProject(newProject, authUser, returning = exception.raiseError[IO, Nothing])

    MultipartRequestEncoder[IO].encode(newProject).map(mp => Request[IO]().withEntity(mp).putHeaders(mp.headers)) >>=
      (req => endpoint.`POST /projects`(req, authUser)) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe Status.InternalServerError) >>
        response.as[Message].asserting(_ shouldBe Message.Error("Creation failed"))
    }
  }

  private implicit val logger: TestLogger[IO] = TestLogger[IO]()
  private val projectCreator = mock[ProjectCreator[IO]]
  private lazy val endpoint  = new EndpointImpl[IO](projectCreator)

  private def givenCreatingProject(newProject: NewProject, authUser: AuthUser, returning: IO[Unit]) =
    (projectCreator.createProject _)
      .expects(newProject, authUser)
      .returning(returning)
}
