/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.webhookservice.hookcreation

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.projects.Id
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import io.renku.webhookservice.CommitSyncRequestSender
import io.renku.webhookservice.WebhookServiceGenerators._
import io.renku.webhookservice.crypto.HookTokenCrypto
import io.renku.webhookservice.hookcreation.HookCreator.CreationResult.{HookCreated, HookExisted}
import io.renku.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import io.renku.webhookservice.hookcreation.project.{ProjectInfo, ProjectInfoFinder}
import io.renku.webhookservice.hookvalidation.HookValidator
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import io.renku.webhookservice.model.{CommitSyncRequest, HookToken, Project}
import io.renku.webhookservice.tokenrepository.AccessTokenAssociator
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class HookCreatorSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "createHook" should {

    "return HookCreated if hook does not exists and it was successfully created" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(HookMissing.pure[IO])

      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(projectInfo.pure[IO])

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(serializedHookToken.pure[IO])

      (projectHookCreator
        .create(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(IO.unit)

      (accessTokenAssociator
        .associate(_: Id, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(IO.unit)

      (commitSyncRequestSender.sendCommitSyncRequest _)
        .expects(CommitSyncRequest(Project(projectInfo.id, projectInfo.path)))
        .returning(().pure[IO])

      hookCreation.createHook(projectId, accessToken).unsafeRunSync() shouldBe HookCreated

      logger.expectNoLogs()
    }

    "return HookExisted if hook was already created for that project" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(HookExists.pure[IO])

      hookCreation.createHook(projectId, accessToken).unsafeRunSync() shouldBe HookExisted

      logger.expectNoLogs()
    }

    "log an error if hook validation fails" in new TestCase {

      val exception = exceptions.generateOne
      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(exception.raiseError[IO, HookValidator.HookValidationResult])

      intercept[Exception] {
        hookCreation.createHook(projectId, accessToken).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if project info fetching fails" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(HookMissing.pure[IO])

      val exception = exceptions.generateOne
      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(exception.raiseError[IO, ProjectInfo])

      intercept[Exception] {
        hookCreation.createHook(projectId, accessToken).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if hook token encryption fails" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(HookMissing.pure[IO])

      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(projectInfo.pure[IO])

      val exception = exceptions.generateOne
      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(exception.raiseError[IO, HookTokenCrypto.SerializedHookToken])

      intercept[Exception] {
        hookCreation.createHook(projectId, accessToken).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if hook creation fails" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(HookMissing.pure[IO])

      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(projectInfo.pure[IO])

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(serializedHookToken.pure[IO])

      val exception = exceptions.generateOne
      (projectHookCreator
        .create(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(exception.raiseError[IO, Unit])

      intercept[Exception] {
        hookCreation.createHook(projectId, accessToken).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if associating projectId with accessToken fails" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(HookMissing.pure[IO])

      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(projectInfo.pure[IO])

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(serializedHookToken.pure[IO])

      (projectHookCreator
        .create(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (accessTokenAssociator
        .associate(_: Id, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(exception.raiseError[IO, Unit])

      intercept[Exception] {
        hookCreation.createHook(projectId, accessToken).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "fail return either HookExisted/HookCreated if loading all events fails" in new TestCase {

      (projectHookValidator
        .validateHook(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(HookMissing.pure[IO])

      (projectInfoFinder
        .findProjectInfo(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(projectInfo.pure[IO])

      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(serializedHookToken.pure[IO])

      (projectHookCreator
        .create(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(IO.unit)

      (accessTokenAssociator
        .associate(_: Id, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(IO.unit)

      (commitSyncRequestSender.sendCommitSyncRequest _)
        .expects(CommitSyncRequest(Project(projectInfo.id, projectInfo.path)))
        .returning(exceptions.generateOne.raiseError[IO, Unit])

      hookCreation.createHook(projectId, accessToken).unsafeRunSync() shouldBe HookCreated

      logger.expectNoLogs()
    }
  }

  private trait TestCase {
    val projectInfo         = projectInfos.generateOne
    val projectId           = projectInfo.id
    val projectHookUrl      = projectHookUrls.generateOne
    val serializedHookToken = serializedHookTokens.generateOne
    val accessToken         = accessTokens.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val projectInfoFinder       = mock[ProjectInfoFinder[IO]]
    val projectHookValidator    = mock[HookValidator[IO]]
    val projectHookCreator      = mock[ProjectHookCreator[IO]]
    val hookTokenCrypto         = mock[HookTokenCrypto[IO]]
    val accessTokenAssociator   = mock[AccessTokenAssociator[IO]]
    val commitSyncRequestSender = mock[CommitSyncRequestSender[IO]]

    val hookCreation = new HookCreatorImpl[IO](
      projectHookUrl,
      projectHookValidator,
      projectInfoFinder,
      hookTokenCrypto,
      projectHookCreator,
      accessTokenAssociator,
      commitSyncRequestSender
    )
  }
}
