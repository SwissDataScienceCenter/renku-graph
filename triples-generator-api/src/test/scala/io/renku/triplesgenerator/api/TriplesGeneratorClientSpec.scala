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

package io.renku.triplesgenerator.api

import Generators._
import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.CustomAsyncIOSpec
import org.http4s.Status.{BadRequest, NotFound}
import org.http4s.Uri
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.typelevel.log4cats.Logger

class TriplesGeneratorClientSpec
    extends AsyncWordSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with ExternalServiceStubbing {

  private implicit val logger: Logger[IO] = TestLogger()
  private lazy val client = new TriplesGeneratorClientImpl[IO](Uri.unsafeFromString(externalServiceBaseUrl))

  "createProject" should {

    "succeed if sending project creation payload to the TG's Project Create API returned Ok" in {

      val newProject = newProjectsGen.generateOne

      stubFor {
        post(urlEqualTo("/projects"))
          .withRequestBody(equalToJson(newProject.asJson.spaces2))
          .willReturn(ok())
      }

      client.createProject(newProject).asserting(_ shouldBe TriplesGeneratorClient.Result.success(()))
    }

    "failed if sending the payload returns other status" in {

      val newProject = newProjectsGen.generateOne

      stubFor {
        post(urlEqualTo("/projects"))
          .willReturn(aResponse.withStatus(BadRequest.code))
      }

      client.createProject(newProject).asserting(_ shouldBe a[TriplesGeneratorClient.Result.Failure])
    }
  }

  "updateProject" should {

    "succeed if sending project update to the TG's Project Update API returned Ok" in {

      val slug    = projectSlugs.generateOne
      val updates = projectUpdatesGen.generateOne

      stubFor {
        patch(urlEqualTo(s"/projects/${urlEncode(slug.value)}"))
          .withRequestBody(equalToJson(updates.asJson.spaces2))
          .willReturn(ok())
      }

      client.updateProject(slug, updates).asserting(_ shouldBe TriplesGeneratorClient.Result.success(()))
    }

    "failed if sending the update returns other status" in {

      val slug    = projectSlugs.generateOne
      val updates = projectUpdatesGen.generateOne

      stubFor {
        patch(urlEqualTo(s"/projects/${urlEncode(slug.value)}"))
          .willReturn(aResponse.withStatus(NotFound.code))
      }

      client
        .updateProject(slug, updates)
        .asserting(_ shouldBe TriplesGeneratorClient.Result.failure("Project for update does not exist"))
    }
  }
}
