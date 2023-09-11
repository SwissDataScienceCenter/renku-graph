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

import Generators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.core.client.Generators.{newProjectsGen => corePayloads}
import io.renku.core.client.{RenkuCoreClient, NewProject => CorePayload, Result => CoreResult}
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings}
import io.renku.http.client.UserAccessToken
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.Failure
import io.renku.knowledgegraph.Generators.failures
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
    "wait for the events to be processed" in {

      val newProject  = newProjects.generateOne
      val authUser    = authUsers.generateOne
      val accessToken = authUser.accessToken

      val corePayload = corePayloads.generateOne
      givenCorePayloadFinding(newProject, authUser, returning = corePayload.pure[IO])

      val glCreatedProject = glCreatedProjectsGen.generateOne
      givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

      givenCoreProjectCreation(corePayload, accessToken, returning = CoreResult.success(()).pure[IO])

      givenHookCreation(glCreatedProject,
                        accessToken,
                        returning = WSResult.success(successfulHookCreationResults.generateOne).pure[IO]
      )

      creator.createProject(newProject, authUser).assertNoException
    }

  it should "fail with the failure returned by project creation in GL" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, authUser, returning = corePayload.pure[IO])

    val failure = failures.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = failure.asLeft.pure[IO])

    creator.createProject(newProject, authUser).assertThrowsError[Exception](_ shouldBe failure)
  }

  it should "fail if creating project in GL failed" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, authUser, returning = corePayload.pure[IO])

    val failure = exceptions.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = failure.raiseError[IO, Nothing])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onGLCreation(newProject.slug, failure))
  }

  it should "fail if project creation in Core returns a failure" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, authUser, returning = corePayload.pure[IO])

    val glCreatedProject = glCreatedProjectsGen.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

    val failure = CoreResult.Failure.Simple(nonEmptyStrings().generateOne)
    givenCoreProjectCreation(corePayload, accessToken, returning = failure.pure[IO])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onCoreCreation(newProject.slug, failure))
  }

  it should "fail if creating project in Core failed" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, authUser, returning = corePayload.pure[IO])

    val glCreatedProject = glCreatedProjectsGen.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

    val failure = exceptions.generateOne
    givenCoreProjectCreation(corePayload, accessToken, returning = failure.raiseError[IO, Nothing])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onCoreCreation(newProject.slug, failure))
  }

  it should "fail if project activation returns a failure" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, authUser, returning = corePayload.pure[IO])

    val glCreatedProject = glCreatedProjectsGen.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

    givenCoreProjectCreation(corePayload, accessToken, returning = CoreResult.success(()).pure[IO])

    val failure = WSResult.Failure(nonEmptyStrings().generateOne)
    givenHookCreation(glCreatedProject, accessToken, returning = failure.pure[IO])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onActivation(newProject.slug, failure))
  }

  it should "fail if activating project fails" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, authUser, returning = corePayload.pure[IO])

    val glCreatedProject = glCreatedProjectsGen.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

    givenCoreProjectCreation(corePayload, accessToken, returning = CoreResult.success(()).pure[IO])

    val failure = exceptions.generateOne
    givenHookCreation(glCreatedProject, accessToken, returning = failure.raiseError[IO, Nothing])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.onActivation(newProject.slug, failure))
  }

  it should "fail if project activation returns NotFound" in {

    val newProject  = newProjects.generateOne
    val authUser    = authUsers.generateOne
    val accessToken = authUser.accessToken

    val corePayload = corePayloads.generateOne
    givenCorePayloadFinding(newProject, authUser, returning = corePayload.pure[IO])

    val glCreatedProject = glCreatedProjectsGen.generateOne
    givenGLProjectCreation(newProject, accessToken, returning = glCreatedProject.asRight.pure[IO])

    givenCoreProjectCreation(corePayload, accessToken, returning = CoreResult.success(()).pure[IO])

    givenHookCreation(glCreatedProject, accessToken, returning = WSResult.success(HookCreationResult.NotFound).pure[IO])

    creator
      .createProject(newProject, authUser)
      .assertThrowsError[Exception](_ shouldBe CreationFailures.activationReturningNotFound(newProject))
  }

  private val glProjectCreator  = mock[GLProjectCreator[IO]]
  private val corePayloadFinder = mock[CorePayloadFinder[IO]]
  private val coreClient        = mock[RenkuCoreClient[IO]]
  private val wsClient          = mock[WebhookServiceClient[IO]]
  private lazy val creator      = new ProjectCreatorImpl[IO](glProjectCreator, corePayloadFinder, coreClient, wsClient)

  private def givenGLProjectCreation(newProject:  NewProject,
                                     accessToken: UserAccessToken,
                                     returning:   IO[Either[Failure, GLCreatedProject]]
  ) = (glProjectCreator.createProject _)
    .expects(newProject, accessToken)
    .returning(returning)

  private def givenCorePayloadFinding(newProject: NewProject, authUser: AuthUser, returning: IO[CorePayload]) =
    (corePayloadFinder.findCorePayload _)
      .expects(newProject, authUser)
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
}
