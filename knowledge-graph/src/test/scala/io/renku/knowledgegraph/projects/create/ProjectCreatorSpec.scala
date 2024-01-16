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

package io.renku.knowledgegraph.projects.create

import Generators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.core.client.Generators.{newProjectsGen => corePayloads}
import io.renku.core.client.{RenkuCoreClient, NewProject => CorePayload, Result => CoreResult}
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings}
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, UserAccessToken}
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.Failure
import io.renku.knowledgegraph.Generators.failures
import io.renku.knowledgegraph.projects.delete.ProjectRemover
import io.renku.triplesgenerator.api.TriplesGeneratorClient.{Result => TGResult}
import io.renku.triplesgenerator.api.{TriplesGeneratorClient, NewProject => TGNewProject}
import io.renku.webhookservice.api.Generators.successfulHookCreationResults
import io.renku.webhookservice.api.WebhookServiceClient.{Result => WSResult}
import io.renku.webhookservice.api.{HookCreationResult, WebhookServiceClient}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProjectCreatorSpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "create the project in GL, " +
    "find payload for Core, " +
    "create the project in Core, " +
    "activate it with the webhook-service and " +
    "create the project in TG" in {

      val newProject  = newProjects.generateOne
      val authUser    = authUsers.generateOne
      val accessToken = authUser.accessToken

      val glCreatedProject = glCreatedProjectsGen.generateOne
      givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

      val corePayload = corePayloads.generateOne
      givenCorePayloadFinding(newProject, glCreatedProject, authUser, returning = corePayload.pure[IO])

      givenCoreProjectCreation(corePayload, accessToken, returning = CoreResult.success(()).pure[IO])

      givenHookCreation(glCreatedProject,
                        accessToken,
                        returning = WSResult.success(successfulHookCreationResults.generateOne).pure[IO]
      )

      givenTGProjectCreation(newProject, glCreatedProject, returning = TGResult.success(()).pure[IO])

      creator.createProject(newProject, authUser).asserting(_ shouldBe glCreatedProject.slug)
    }

  it should "fail with the failure returned by project creation in GL" in {

    val newProject = newProjects.generateOne
    val authUser   = authUsers.generateOne

    val failure = failures.generateOne
    givenGLProjectCreation(newProject, authUser.accessToken, returning = failure.asLeft.pure[IO])

    creator.createProject(newProject, authUser).assertThrowsError[Exception](_ shouldBe failure)
  }

  it should "fail if creating project in GL failed" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val failure = exceptions.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = failure.raiseError[IO, Nothing])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onGLCreation(newProject.name, failure))
  }

  it should "remove the project from GL and fail if project creation in Core returns a failure" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val glCreatedProject = glCreatedProjectsGen.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, glCreatedProject, authUser, returning = corePayload.pure[IO])

    val failure = CoreResult.Failure.Simple(nonEmptyStrings().generateOne)
    givenCoreProjectCreation(corePayload, accessToken, returning = failure.pure[IO])

    givenGLProjectRemover(glCreatedProject, accessToken, returning = ().pure[IO])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onCoreCreation(glCreatedProject.slug, failure))
  }

  it should "remove the project from GL and fail if creating project in Core failed" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val glCreatedProject = glCreatedProjectsGen.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, glCreatedProject, authUser, returning = corePayload.pure[IO])

    val failure = exceptions.generateOne
    givenCoreProjectCreation(corePayload, accessToken, returning = failure.raiseError[IO, Nothing])

    givenGLProjectRemover(glCreatedProject, accessToken, returning = ().pure[IO])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onCoreCreation(glCreatedProject.slug, failure))
  }

  it should "fail with the Core failure if removing project from GL failed, too" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val glCreatedProject = glCreatedProjectsGen.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, glCreatedProject, authUser, returning = corePayload.pure[IO])

    val failure = exceptions.generateOne
    givenCoreProjectCreation(corePayload, accessToken, returning = failure.raiseError[IO, Nothing])

    givenGLProjectRemover(glCreatedProject, accessToken, returning = exceptions.generateOne.raiseError[IO, Nothing])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onCoreCreation(glCreatedProject.slug, failure))
  }

  it should "fail if project activation returns a failure" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val glCreatedProject = glCreatedProjectsGen.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, glCreatedProject, authUser, returning = corePayload.pure[IO])

    givenCoreProjectCreation(corePayload, accessToken, returning = CoreResult.success(()).pure[IO])

    val failure = WSResult.Failure(nonEmptyStrings().generateOne)
    givenHookCreation(glCreatedProject, accessToken, returning = failure.pure[IO])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onActivation(glCreatedProject.slug, failure))
  }

  it should "fail if activating project fails" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val glCreatedProject = glCreatedProjectsGen.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, glCreatedProject, authUser, returning = corePayload.pure[IO])

    givenCoreProjectCreation(corePayload, accessToken, returning = CoreResult.success(()).pure[IO])

    val failure = exceptions.generateOne
    givenHookCreation(glCreatedProject, accessToken, returning = failure.raiseError[IO, Nothing])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onActivation(glCreatedProject.slug, failure))
  }

  it should "fail if project activation returns NotFound" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val glCreatedProject = glCreatedProjectsGen.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, glCreatedProject, authUser, returning = corePayload.pure[IO])

    givenCoreProjectCreation(corePayload, accessToken, returning = CoreResult.success(()).pure[IO])

    givenHookCreation(glCreatedProject, accessToken, returning = WSResult.success(HookCreationResult.NotFound).pure[IO])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.activationReturningNotFound(glCreatedProject.slug))
  }

  it should "fail with the failure returned by project creation in TG" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val glCreatedProject = glCreatedProjectsGen.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, glCreatedProject, authUser, returning = corePayload.pure[IO])

    givenCoreProjectCreation(corePayload, accessToken, returning = CoreResult.success(()).pure[IO])

    givenHookCreation(glCreatedProject, accessToken, returning = WSResult.success(HookCreationResult.Created).pure[IO])

    val failure = TGResult.Failure(nonEmptyStrings().generateOne)
    givenTGProjectCreation(newProject, glCreatedProject, returning = failure.pure[IO])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onTGCreation(glCreatedProject.slug, failure))
  }

  it should "fail if creating project in TG failed" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val glCreatedProject = glCreatedProjectsGen.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, glCreatedProject, authUser, returning = corePayload.pure[IO])

    givenCoreProjectCreation(corePayload, accessToken, returning = CoreResult.success(()).pure[IO])

    givenHookCreation(glCreatedProject, accessToken, returning = WSResult.success(HookCreationResult.Created).pure[IO])

    val failure = exceptions.generateOne
    givenTGProjectCreation(newProject, glCreatedProject, returning = failure.raiseError[IO, Nothing])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onTGCreation(glCreatedProject.slug, failure))
  }

  private implicit val logger: TestLogger[IO] = TestLogger[IO]()
  private val glProjectCreator  = mock[GLProjectCreator[IO]]
  private val corePayloadFinder = mock[CorePayloadFinder[IO]]
  private val coreClient        = mock[RenkuCoreClient[IO]]
  private val wsClient          = mock[WebhookServiceClient[IO]]
  private val tgClient          = mock[TriplesGeneratorClient[IO]]
  private val glProjectRemover  = mock[ProjectRemover[IO]]
  private lazy val creator =
    new ProjectCreatorImpl[IO](glProjectCreator, corePayloadFinder, coreClient, wsClient, tgClient, glProjectRemover)

  private def givenGLProjectCreation(newProject:  NewProject,
                                     accessToken: UserAccessToken,
                                     returning:   IO[Either[Failure, GLCreatedProject]]
  ) = (glProjectCreator.createProject _)
    .expects(newProject, accessToken)
    .returning(returning)

  private def givenCorePayloadFinding(newProject: NewProject,
                                      glCreated:  GLCreatedProject,
                                      authUser:   AuthUser,
                                      returning:  IO[CorePayload]
  ) = (corePayloadFinder.findCorePayload _)
    .expects(newProject, glCreated, authUser)
    .returning(returning)

  private def givenCoreProjectCreation(corePayload: CorePayload,
                                       accessToken: UserAccessToken,
                                       returning:   IO[CoreResult[Unit]]
  ) = (coreClient.createProject _)
    .expects(corePayload, accessToken)
    .returning(returning)

  private def givenHookCreation(glCreatedProject: GLCreatedProject,
                                accessToken:      UserAccessToken,
                                returning:        IO[WSResult[HookCreationResult]]
  ) = (wsClient.createHook _)
    .expects(glCreatedProject.id, accessToken)
    .returning(returning)

  private def givenGLProjectRemover(glCreatedProject: GLCreatedProject,
                                    accessToken:      UserAccessToken,
                                    returning:        IO[Unit]
  ) = (glProjectRemover
    .deleteProject(_: projects.GitLabId)(_: AccessToken))
    .expects(glCreatedProject.id, accessToken)
    .returning(returning)

  private def givenTGProjectCreation(newProject: NewProject,
                                     glCreated:  GLCreatedProject,
                                     returning:  IO[TGResult[Unit]]
  ) = (tgClient.createProject _)
    .expects(
      TGNewProject(
        newProject.name,
        glCreated.slug,
        newProject.maybeDescription,
        glCreated.dateCreated,
        TGNewProject.Creator(glCreated.creator.name, glCreated.creator.id),
        newProject.keywords,
        newProject.visibility,
        glCreated.maybeImage.toList
      )
    )
    .returning(returning)
}
