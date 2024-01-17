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

package io.renku.core.client

import Generators._
import cats.MonadThrow
import cats.effect.IO
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.http.client.GitLabGenerators.{accessTokens, userAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.{projectGitHttpUrls, projectSchemaVersions}
import io.renku.graph.model.projects
import io.renku.graph.model.versions.SchemaVersion
import io.renku.http.client.{AccessToken, UserAccessToken}
import io.renku.interpreters.TestLogger
import io.renku.testtools.CustomAsyncIOSpec
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{EitherValues, OptionValues}
import org.typelevel.log4cats.Logger

class RenkuCoreClientSpec
    extends AsyncWordSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with OptionValues
    with EitherValues
    with AsyncMockFactory {

  "findCoreUri(projects.GitHttpUrl)" should {

    "call the getVersions and " +
      "for the first version find the CoreUri in the config and " +
      "call the getMigrationCheck on the found uri and " +
      "if the schema versions match and migration is not required " +
      "return uri from the call to findCoreUri(SchemaVersion)" in {

        val schemaVersion = projectSchemaVersions.generateOne
        givenVersionsFinding(returning = Result.success(List(schemaVersion, projectSchemaVersions.generateOne)))

        val coreUriForSchema = coreUrisForSchema.generateOne.copy(schemaVersion = schemaVersion)
        givenCoreUriForSchemaInConfig(coreUriForSchema)

        val projectUrl     = projectGitHttpUrls.generateOne
        val userInfo       = userInfos.generateOne
        val accessToken    = accessTokens.generateOne
        val migrationCheck = ProjectMigrationCheck(schemaVersion, MigrationRequired.no)

        givenMigrationCheckFetching(coreUriForSchema,
                                    projectUrl,
                                    userInfo,
                                    accessToken,
                                    returning = Result.success(migrationCheck)
        )

        val coreUriVersioned = givenFindCoreUri(migrationCheck.schemaVersion)

        client
          .findCoreUri(projectUrl, userInfo, accessToken)
          .asserting(_ shouldBe Result.success(coreUriVersioned))
      }

    "call the getVersions and " +
      "call the getMigrationCheck for each schema and uri " +
      "until the schema versions match and migration is not required and " +
      "return the uri from the call to findCoreUri(SchemaVersion)" in {

        val schemaVersion1 = projectSchemaVersions.generateOne
        val schemaVersion2 = projectSchemaVersions.generateOne
        val schemaVersion3 = projectSchemaVersions.generateOne
        givenVersionsFinding(returning = Result.success(List(schemaVersion1, schemaVersion2, schemaVersion3)))

        val coreUriForSchema1 = coreUrisForSchema.generateOne.copy(schemaVersion = schemaVersion1)
        givenCoreUriForSchemaInConfig(coreUriForSchema1)
        val coreUriForSchema2 = coreUrisForSchema.generateOne.copy(schemaVersion = schemaVersion2)
        givenCoreUriForSchemaInConfig(coreUriForSchema2)
        val coreUriForSchema3 = coreUrisForSchema.generateOne.copy(schemaVersion = schemaVersion3)
        givenCoreUriForSchemaInConfig(coreUriForSchema3)

        val projectUrl  = projectGitHttpUrls.generateOne
        val userInfo    = userInfos.generateOne
        val accessToken = accessTokens.generateOne

        // case when Migration not required by different schema version - a weird case
        val migrationCheck1 = ProjectMigrationCheck(projectSchemaVersions.generateOne, MigrationRequired.no)
        givenMigrationCheckFetching(coreUriForSchema1,
                                    projectUrl,
                                    userInfo,
                                    accessToken,
                                    returning = Result.success(migrationCheck1)
        )

        // case when Migration required for the schema version
        val migrationCheck2 = ProjectMigrationCheck(schemaVersion2, MigrationRequired.yes)
        givenMigrationCheckFetching(coreUriForSchema2,
                                    projectUrl,
                                    userInfo,
                                    accessToken,
                                    returning = Result.success(migrationCheck2)
        )
        // case when Migration not required for the schema version
        val migrationCheck3 = ProjectMigrationCheck(schemaVersion3, MigrationRequired.no)
        givenMigrationCheckFetching(coreUriForSchema3,
                                    projectUrl,
                                    userInfo,
                                    accessToken,
                                    returning = Result.success(migrationCheck3)
        )

        val coreUriVersioned = givenFindCoreUri(migrationCheck3.schemaVersion)

        client
          .findCoreUri(projectUrl, userInfo, accessToken)
          .asserting(_ shouldBe Result.success(coreUriVersioned))
      }

    "fail when no getMigrationCheck returning matching schemaVersion and no migration requirements" in {

      val schemaVersions = projectSchemaVersions.generateList()
      givenVersionsFinding(returning = Result.success(schemaVersions))

      val projectUrl  = projectGitHttpUrls.generateOne
      val userInfo    = userInfos.generateOne
      val accessToken = accessTokens.generateOne

      schemaVersions foreach { sv =>
        val coreUriForSchema = coreUrisForSchema.generateOne.copy(schemaVersion = sv)
        givenCoreUriForSchemaInConfig(coreUriForSchema)

        givenMigrationCheckFetching(coreUriForSchema,
                                    projectUrl,
                                    userInfo,
                                    accessToken,
                                    returning = Result.success(projectMigrationChecks.generateOne)
        )
      }

      client
        .findCoreUri(projectUrl, userInfo, accessToken)
        .asserting(_ shouldBe Result.failure("Project in unsupported version. Quite likely migration required"))
    }

    "fail if any of the calls towards the Core API fails" in {

      val schemaVersion = projectSchemaVersions.generateOne
      givenVersionsFinding(returning = Result.success(List(schemaVersion)))

      val coreUriForSchema = coreUrisForSchema.generateOne.copy(schemaVersion = schemaVersion)
      givenCoreUriForSchemaInConfig(coreUriForSchema)

      val projectUrl  = projectGitHttpUrls.generateOne
      val userInfo    = userInfos.generateOne
      val accessToken = accessTokens.generateOne

      val failure = resultDetailedFailures.generateOne
      givenMigrationCheckFetching(coreUriForSchema, projectUrl, userInfo, accessToken, returning = failure)

      client.findCoreUri(projectUrl, userInfo, accessToken).asserting(_ shouldBe failure)
    }
  }

  "findCoreUri(SchemaVersion)" should {

    "find the uri of the core for the given schema in the config, " +
      "fetch the api version using the uri and " +
      "return the CoreUri relevant for the given schema" in {

        val schemaVersion = projectSchemaVersions.generateOne

        val expectedUriVersioned = givenFindCoreUri(schemaVersion)

        client
          .findCoreUri(schemaVersion)
          .asserting(_ shouldBe Result.success(expectedUriVersioned))
      }

    "fail if finding the uri of the core for the given schema in the config fails " in {

      val exception = exceptions.generateOne
      givenCoreUriForSchemaInConfig(failsWith = exception)

      val schemaVersion = projectSchemaVersions.generateOne

      client
        .findCoreUri(schemaVersion)
        .assertThrowsError[Exception](_ shouldBe exception)
    }

    "return a failure if fetching the api version fails" in {

      val coreUriForSchema = coreUrisForSchema.generateOne
      val failure          = resultDetailedFailures.generateOne

      givenCoreUriForSchemaInConfig(returning = coreUriForSchema)
      givenApiVersionFetching(coreUriForSchema, returning = failure)

      client.findCoreUri(coreUriForSchema.schemaVersion).asserting(_ shouldBe failure)
    }
  }

  "createProject" should {

    "call the Core's postProjectCreate API" in {

      val newProject  = newProjectsGen.generateOne
      val accessToken = userAccessTokens.generateOne

      val result = resultsGen(()).generateOne
      givenPostingProjectCreate(newProject, accessToken, returning = result)

      client.createProject(newProject, accessToken).asserting(_ shouldBe result)
    }
  }

  "updateProject" should {

    "call the Core's postProjectUpdate API" in {

      val coreUri     = coreUrisVersioned.generateOne
      val updates     = projectUpdatesGen.generateOne
      val accessToken = userAccessTokens.generateOne

      val result = resultsGen(branches).generateOne
      givenPostingProjectUpdate(coreUri, updates, accessToken, returning = result)

      client.updateProject(coreUri, updates, accessToken).asserting(_ shouldBe result)
    }
  }

  private implicit val logger: Logger[IO] = TestLogger()
  private val coreUriForSchemaLoader = mock[RenkuCoreUri.ForSchemaLoader]
  private val lowLevelApis           = mock[LowLevelApis[IO]]
  private lazy val config            = mock[Config]
  private lazy val client            = new RenkuCoreClientImpl[IO](coreUriForSchemaLoader, lowLevelApis, config)

  private def givenFindCoreUri(schemaVersion: SchemaVersion) = {

    val coreUriForSchema = coreUrisForSchema.generateOne.copy(schemaVersion = schemaVersion)
    givenCoreUriForSchemaInConfig(returning = coreUriForSchema)

    val apiVersions = schemaApiVersions.generateOne
    givenApiVersionFetching(coreUriForSchema, returning = Result.success(apiVersions))

    RenkuCoreUri.Versioned(coreUriForSchema, apiVersions.max)
  }

  private def givenApiVersionFetching(coreUri: RenkuCoreUri.ForSchema, returning: Result[SchemaApiVersions]) =
    (lowLevelApis.getApiVersion _)
      .expects(coreUri)
      .returning(returning.pure[IO])

  private def givenMigrationCheckFetching(coreUri:     RenkuCoreUri,
                                          projectUrl:  projects.GitHttpUrl,
                                          userInfo:    UserInfo,
                                          accessToken: AccessToken,
                                          returning:   Result[ProjectMigrationCheck]
  ) = (lowLevelApis.getMigrationCheck _)
    .expects(coreUri, projectUrl, userInfo, accessToken)
    .returning(returning.pure[IO])

  private def givenVersionsFinding(returning: Result[List[SchemaVersion]]) =
    (() => lowLevelApis.getVersions)
      .expects()
      .returning(returning.pure[IO])

  private def givenCoreUriForSchemaInConfig(returning: RenkuCoreUri.ForSchema) =
    (coreUriForSchemaLoader
      .loadFromConfig[IO](_: SchemaVersion, _: Config)(_: MonadThrow[IO]))
      .expects(returning.schemaVersion, config, *)
      .returning(returning.pure[IO])

  private def givenCoreUriForSchemaInConfig(failsWith: Throwable) =
    (coreUriForSchemaLoader
      .loadFromConfig[IO](_: SchemaVersion, _: Config)(_: MonadThrow[IO]))
      .expects(*, config, *)
      .returning(failsWith.raiseError[IO, Nothing])

  private def givenPostingProjectCreate(newProject: NewProject, accessToken: UserAccessToken, returning: Result[Unit]) =
    (lowLevelApis.postProjectCreate _)
      .expects(newProject, accessToken)
      .returning(returning.pure[IO])

  private def givenPostingProjectUpdate(coreUri:     RenkuCoreUri.Versioned,
                                        updates:     ProjectUpdates,
                                        accessToken: UserAccessToken,
                                        returning:   Result[Branch]
  ) = (lowLevelApis.postProjectUpdate _)
    .expects(coreUri, updates, accessToken)
    .returning(returning.pure[IO])
}
