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

package io.renku.core.client

import Generators._
import TestModelCodecs._
import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.userAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{cliVersions, projectSchemaVersions}
import io.renku.graph.model.RenkuTinyTypeGenerators.projectGitHttpUrls
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.CustomAsyncIOSpec
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{EitherValues, OptionValues}
import org.typelevel.log4cats.Logger

class LowLevelApisSpec
    extends AsyncWordSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with OptionValues
    with EitherValues
    with ExternalServiceStubbing
    with AsyncMockFactory {

  "getApiVersion" should {

    "return info about API versions" in {

      val successResult = resultSuccesses(schemaApiVersions).generateOne

      otherWireMockResource.evalMap { server =>
        server.stubFor {
          get(s"/renku/apiversion")
            .willReturn(ok(successResult.asJson.spaces2))
        }

        val uriForSchema = RenkuCoreUri.ForSchema(server.baseUri, projectSchemaVersions.generateOne)

        client.getApiVersion(uriForSchema).asserting(_ shouldBe successResult)
      }.use_
    }
  }

  "getMigrationCheck" should {

    "return info about migration status of the given project on the given Core API" in {

      val accessToken       = userAccessTokens.generateOne
      val projectGitHttpUrl = projectGitHttpUrls.generateOne
      val migrationCheck    = projectMigrationChecks.generateOne

      otherWireMockResource.evalMap { server =>
        server.stubFor {
          get(urlPathEqualTo("/renku/cache.migrations_check"))
            .withQueryParam("git_url", equalTo(projectGitHttpUrl.value))
            .withHeader("gitlab-token", equalTo(accessToken.value))
            .willReturn(ok(Result.success(migrationCheck).asJson.spaces2))
        }

        val uriForSchema = RenkuCoreUri.ForSchema(server.baseUri, projectSchemaVersions.generateOne)

        client
          .getMigrationCheck(uriForSchema, projectGitHttpUrl, accessToken)
          .asserting(_ shouldBe Result.success(migrationCheck))
      }.use_
    }
  }

  "getVersions" should {

    "return info about available versions" in {

      val versionTuples = (projectSchemaVersions -> cliVersions).mapN(_ -> _).generateList()

      stubFor {
        get(s"/renku/versions")
          .willReturn(ok(Result.success(versionTuples).asJson.spaces2))
      }

      client.getVersions.asserting(_ shouldBe Result.success(versionTuples.map(_._1)))
    }
  }

  "postProjectCreate" should {

    "do POST /renku/templates.create_project with a relevant payload" in {

      val accessToken = userAccessTokens.generateOne
      val newProject  = newProjectsGen.generateOne

      stubFor {
        post(s"/renku/templates.create_project")
          .withRequestBody(equalToJson(newProject.asJson.spaces2))
          .withAccessToken(accessToken.some)
          .withHeader("renku-user-email", equalTo(newProject.userInfo.email.value))
          .withHeader("renku-user-fullname", equalTo(newProject.userInfo.name.value))
          .willReturn(ok(Result.success(json"""{"name": ${newProject.name}}""").asJson.spaces2))
      }

      client.postProjectCreate(newProject, accessToken).asserting(_ shouldBe Result.success(()))
    }
  }

  "postProjectUpdate" should {

    "do POST /renku/project.edit with a relevant payload" in {

      val accessToken = userAccessTokens.generateOne
      val updates     = projectUpdatesGen.generateOne

      otherWireMockResource.evalMap { server =>
        val versionedUri = coreUrisVersioned(server.baseUri).generateOne
        val remoteBranch = branches.generateOne

        server.stubFor {
          post(s"/renku/${versionedUri.apiVersion}/project.edit")
            .withRequestBody(equalToJson(updates.asJson.spaces2))
            .withAccessToken(accessToken.some)
            .withHeader("renku-user-email", equalTo(updates.userInfo.email.value))
            .withHeader("renku-user-fullname", equalTo(updates.userInfo.name.value))
            .willReturn(ok(Result.success(json"""{"edited": {}, "remote_branch": $remoteBranch}""").asJson.spaces2))
        }

        client
          .postProjectUpdate(versionedUri, updates, accessToken)
          .asserting(_ shouldBe Result.success(remoteBranch))
      }.use_
    }
  }

  private implicit val logger: Logger[IO] = TestLogger()
  private lazy val client = new LowLevelApisImpl[IO](RenkuCoreUri.Latest(externalServiceBaseUri), ClientTools[IO])
}
