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

package io.renku.webhookservice.hookcreation

import cats.effect.IO
import cats.effect.std.Queue
import cats.syntax.all._
import io.renku.eventlog
import io.renku.eventlog.api.events.{CommitSyncRequest, GlobalCommitSyncRequest, StatusChangeEvent}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import io.renku.webhookservice.ProjectInfoFinder
import io.renku.webhookservice.WebhookServiceGenerators._
import io.renku.webhookservice.crypto.HookTokenCrypto
import io.renku.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import io.renku.webhookservice.hookcreation.HookCreator.CreationResult.{HookCreated, HookExisted}
import io.renku.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import io.renku.webhookservice.hookvalidation.HookValidator
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import io.renku.webhookservice.model.HookToken
import io.renku.webhookservice.tokenrepository.AccessTokenAssociator
import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

class HookCreatorSpec
    extends AnyWordSpec
    with MockFactory
    with should.Matchers
    with OptionValues
    with IOSpec
    with Eventually {

  "createHook" should {

    "return HookCreated if hook does not exists and it was successfully created" in new TestCase {

      givenHookValidation(returning = HookMissing.some.pure[IO])

      givenTokenAssociation(returning = IO.unit)

      givenTokenEncryption(returning = serializedHookToken.pure[IO])

      givenHookCreation(returning = IO.unit)

      givenProjectInfoFinding(returning = projectInfo.some.pure[IO])

      givenCommitSyncRequestSending(returning = IO.unit)

      hookCreation.createHook(projectId, authUser).unsafeRunSync().value shouldBe HookCreated

      validateCommitSyncRequestSent

      logger.loggedOnly(Info(show"Hook created for projectId $projectId"))
    }

    "return HookExisted if hook was already created for that project" in new TestCase {

      givenHookValidation(returning = HookExists.some.pure[IO])

      givenProjectInfoFinding(returning = projectInfo.some.pure[IO])

      givenCommitSyncRequestSending(returning = ().pure[IO])

      hookCreation.createHook(projectId, authUser).unsafeRunSync().value shouldBe HookExisted

      validateCommitSyncRequestSent

      logger.loggedOnly(Info(show"Hook existed for projectId $projectId"))
    }

    "return None if hook validation returns None" in new TestCase {

      givenHookValidation(returning = None.pure[IO])

      hookCreation.createHook(projectId, authUser).unsafeRunSync() shouldBe None

      logger.loggedOnly(Info(show"Hook on projectId $projectId cannot be created by user ${authUser.id}"))
    }

    "log an error if hook validation fails" in new TestCase {

      val exception = exceptions.generateOne
      givenHookValidation(returning = exception.raiseError[IO, Nothing])

      intercept[Exception] {
        hookCreation.createHook(projectId, authUser).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if associating projectId with accessToken fails" in new TestCase {

      givenHookValidation(returning = HookMissing.some.pure[IO])

      val exception = exceptions.generateOne
      givenTokenAssociation(returning = exception.raiseError[IO, Unit])

      intercept[Exception] {
        hookCreation.createHook(projectId, authUser).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if hook token encryption fails" in new TestCase {

      givenHookValidation(returning = HookMissing.some.pure[IO])

      givenTokenAssociation(returning = ().pure[IO])

      val exception = exceptions.generateOne
      givenTokenEncryption(returning = exception.raiseError[IO, HookTokenCrypto.SerializedHookToken])

      intercept[Exception] {
        hookCreation.createHook(projectId, authUser).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error if hook creation fails" in new TestCase {

      givenHookValidation(returning = HookMissing.some.pure[IO])

      givenTokenAssociation(returning = ().pure[IO])

      givenTokenEncryption(returning = serializedHookToken.pure[IO])

      val exception = exceptions.generateOne
      givenHookCreation(returning = exception.raiseError[IO, Unit])

      intercept[Exception] {
        hookCreation.createHook(projectId, authUser).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook creation failed for project with id $projectId", exception))
    }

    "log an error and do not send a COMMIT_SYNC_REQUEST if project info fetching fails" in new TestCase {

      givenHookValidation(returning = HookExists.some.pure[IO])

      val exception = exceptions.generateOne
      givenProjectInfoFinding(returning = exception.raiseError[IO, Option[Project]])

      hookCreation.createHook(projectId, authUser).unsafeRunSync().value shouldBe HookExisted

      eventually {
        logger.loggedOnly(
          Info(show"Hook existed for projectId $projectId"),
          Error(s"Hook creation - COMMIT_SYNC_REQUEST not sent as finding project $projectId failed", exception)
        )
      }
    }
  }

  private trait TestCase {
    val projectInfo            = consumerProjects.generateOne
    val projectId              = projectInfo.id
    val serializedHookToken    = serializedHookTokens.generateOne
    val authUser               = authUsers.generateOne
    private val accessToken    = authUser.accessToken
    private val projectHookUrl = projectHookUrls.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val projectHookValidator      = mock[HookValidator[IO]]
    private val tokenAssociator           = mock[AccessTokenAssociator[IO]]
    private val projectHookCreator        = mock[ProjectHookCreator[IO]]
    private val hookTokenCrypto           = mock[HookTokenCrypto[IO]]
    private val projectInfoFinderResponse = Queue.bounded[IO, IO[Option[Project]]](1).unsafeRunSync()
    private val projectInfoFinder = new ProjectInfoFinder[IO] {
      override def findProjectInfo(projectId: GitLabId)(implicit mat: Option[AccessToken]): IO[Option[Project]] =
        projectInfoFinderResponse.take.flatten
    }
    private val commitSyncRequestSenderResponse = Queue.bounded[IO, IO[Unit]](1).unsafeRunSync()
    private val elClient = new eventlog.api.events.Client[IO] {
      override def send(event: CommitSyncRequest): IO[Unit] = commitSyncRequestSenderResponse.take.flatten
      override def send(event: GlobalCommitSyncRequest): IO[Unit] =
        sys.error(s"${StatusChangeEvent.RedoProjectTransformation} event shouldn't be sent")
      override def send(event: StatusChangeEvent.RedoProjectTransformation): IO[Unit] =
        sys.error(s"${StatusChangeEvent.RedoProjectTransformation} event shouldn't be sent")
    }

    val hookCreation = new HookCreatorImpl[IO](
      projectHookUrl,
      projectHookValidator,
      tokenAssociator,
      projectInfoFinder,
      hookTokenCrypto,
      projectHookCreator,
      elClient
    )

    def givenHookValidation(returning: IO[Option[HookValidationResult]]) =
      (projectHookValidator
        .validateHook(_: GitLabId, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(returning)

    def givenTokenAssociation(returning: IO[Unit]) =
      (tokenAssociator
        .associate(_: GitLabId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(returning)

    def givenTokenEncryption(returning: IO[SerializedHookToken]) =
      (hookTokenCrypto
        .encrypt(_: HookToken))
        .expects(HookToken(projectId))
        .returning(returning)

    def givenHookCreation(returning: IO[Unit]) =
      (projectHookCreator
        .create(_: ProjectHook, _: AccessToken))
        .expects(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
        .returning(returning)

    def givenProjectInfoFinding(returning: IO[Option[Project]]): Unit =
      projectInfoFinderResponse.offer(returning).unsafeRunSync()

    def givenCommitSyncRequestSending(returning: IO[Unit]): Unit =
      commitSyncRequestSenderResponse.offer(returning).unsafeRunSync()

    def validateCommitSyncRequestSent = eventually(
      commitSyncRequestSenderResponse.size.unsafeRunSync() shouldBe 0
    )
  }

  implicit override lazy val patienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = scaled(Span(1, Seconds)),
      interval = scaled(Span(50, Millis))
    )
}
