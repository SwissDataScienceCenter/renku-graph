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
import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, jsons}
import io.renku.graph.model.RenkuTinyTypeGenerators.projectPaths
import io.renku.graph.model.projects
import io.renku.http.ErrorMessage._
import io.renku.http.InfoMessage._
import io.renku.http.client.AccessToken
import io.renku.http.server.EndpointTester._
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.interpreters.TestLogger
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.SyncRepoMetadata
import org.http4s.{Request, Status}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class EndpointSpec extends AsyncFlatSpec with CustomAsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "call the GL's Edit Project API with the new values extracted from the request, " +
    "send a SYNC_REPO_METADATA event to TG " +
    "and return 202 Accepted" in {

      val authUser  = authUsers.generateOne
      val path      = projectPaths.generateOne
      val newValues = newValuesGen.generateOne

      givenUpdatingProjectInGL(path, newValues, authUser.accessToken, returning = EitherT.pure[IO, Json](()))
      givenSyncRepoMetadataSending(path, returning = ().pure[IO])

      endpoint.`PUT /projects/:path`(path, Request[IO]().withEntity(newValues.asJson), authUser) >>= { response =>
        response.pure[IO].asserting(_.status shouldBe Status.Accepted) >>
          response.as[Json].asserting(_ shouldBe InfoMessage("Project update accepted").asJson)
      }
    }

  it should "return 400 BadRequest if payload is malformed" in {

    val authUser = authUsers.generateOne
    val path     = projectPaths.generateOne

    endpoint.`PUT /projects/:path`(path, Request[IO]().withEntity(Json.obj()), authUser) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe Status.BadRequest) >>
        response.as[ErrorMessage].asserting(_ shouldBe ErrorMessage("Invalid payload"))
    }
  }

  it should "return 400 BadRequest if GL returns 400" in {

    val authUser  = authUsers.generateOne
    val path      = projectPaths.generateOne
    val newValues = newValuesGen.generateOne

    val error = jsons.generateOne
    givenUpdatingProjectInGL(path, newValues, authUser.accessToken, returning = EitherT.left(error.pure[IO]))

    endpoint.`PUT /projects/:path`(path, Request[IO]().withEntity(newValues.asJson), authUser) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe Status.BadRequest) >>
        response.as[ErrorMessage].asserting(_ shouldBe ErrorMessage(error))
    }
  }

  it should "return 500 InternalServerError if updating GL failed" in {

    val authUser  = authUsers.generateOne
    val path      = projectPaths.generateOne
    val newValues = newValuesGen.generateOne

    val exception = exceptions.generateOne
    givenUpdatingProjectInGL(path,
                             newValues,
                             authUser.accessToken,
                             returning = EitherT(exception.raiseError[IO, Either[Json, Unit]])
    )

    endpoint.`PUT /projects/:path`(path, Request[IO]().withEntity(newValues.asJson), authUser) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe Status.InternalServerError) >>
        response.as[Json].asserting(_ shouldBe ErrorMessage("Update failed").asJson)
    }
  }

  it should "return 500 InternalServerError if sending event failed" in {

    val authUser  = authUsers.generateOne
    val path      = projectPaths.generateOne
    val newValues = newValuesGen.generateOne

    givenUpdatingProjectInGL(path, newValues, authUser.accessToken, returning = EitherT.pure[IO, Json](()))
    val exception = exceptions.generateOne
    givenSyncRepoMetadataSending(path, returning = exception.raiseError[IO, Nothing])

    endpoint.`PUT /projects/:path`(path, Request[IO]().withEntity(newValues.asJson), authUser) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe Status.InternalServerError) >>
        response.as[Json].asserting(_ shouldBe ErrorMessage("Update failed").asJson)
    }
  }

  private implicit val logger: TestLogger[IO] = TestLogger[IO]()
  private val glProjectUpdater = mock[GLProjectUpdater[IO]]
  private val tgClient         = mock[triplesgenerator.api.events.Client[IO]]
  private lazy val endpoint    = new EndpointImpl[IO](glProjectUpdater, tgClient)

  private def givenUpdatingProjectInGL(path:      projects.Path,
                                       newValues: NewValues,
                                       at:        AccessToken,
                                       returning: EitherT[IO, Json, Unit]
  ) = (glProjectUpdater.updateProject _)
    .expects(path, newValues, at)
    .returning(returning)

  private def givenSyncRepoMetadataSending(path: projects.Path, returning: IO[Unit]) =
    (tgClient.send(_: SyncRepoMetadata)).expects(SyncRepoMetadata(path)).returning(returning)

  private implicit lazy val payloadEncoder: Encoder[NewValues] = Encoder.instance { case NewValues(visibility) =>
    json"""{"visibility":  $visibility}"""
  }
}
