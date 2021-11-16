package io.renku.webhookservice.hookdeletion

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits.GenOps
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects.Id
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import io.renku.webhookservice.hookdeletion.HookDeletor.DeletionResult
import io.renku.webhookservice.hookdeletion.HookDeletor.DeletionResult.{HookDeleted, HookNotFound}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class HookDeletorSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "deleteHook" should {

    "return HookeDeleted if hook exists and it was successfully deleted" in new TestCase {

      (projectHookDeletor
        .delete(_: Id, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(HookDeleted.pure[IO])

      hookDeletor.deleteHook(projectId, accessToken).unsafeRunSync() shouldBe HookDeleted

      logger.expectNoLogs()
    }

    "return HookNotFound if hook was already removed for that project" in new TestCase {

      (projectHookDeletor
        .delete(_: Id, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(HookNotFound.pure[IO])

      hookDeletor.deleteHook(projectId, accessToken).unsafeRunSync() shouldBe HookNotFound

      logger.expectNoLogs()
    }

    "log an error if hook deletion fails" in new TestCase {

      val exception = exceptions.generateOne
      (projectHookDeletor
        .delete(_: Id, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(exception.raiseError[IO, DeletionResult])

      intercept[Exception] {
        hookDeletor.deleteHook(projectId, accessToken).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook deletion failed for project with id $projectId", exception))
    }
  }

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val accessToken = accessTokens.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val projectHookDeletor = mock[ProjectHookDeletor[IO]]

    val hookDeletor = new HookDeletorImpl[IO](projectHookDeletor)
  }
}
