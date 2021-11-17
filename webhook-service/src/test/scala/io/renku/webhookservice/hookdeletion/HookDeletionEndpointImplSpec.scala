package io.renku.webhookservice.hookdeletion

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.circe.syntax.EncoderOps
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits.GenOps
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.http.client.AccessToken
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import io.renku.webhookservice.WebhookServiceGenerators.projectHookIds
import io.renku.webhookservice.hookdeletion.HookDeletor.DeletionResult.{HookDeleted, HookNotFound}
import io.renku.webhookservice.model.HookIdentifier
import org.http4s.MediaType
import org.http4s.Status.{InternalServerError, NotFound, Ok, Unauthorized}
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class HookDeletionEndpointImplSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {
  "deleteHook" should {

    "return OK when webhook is successfully deleted for project with the given id in GitLab" in new TestCase {

      (hookDeletor
        .deleteHook(_: HookIdentifier, _: AccessToken))
        .expects(projectHookId, authUser.accessToken)
        .returning(HookDeleted.pure[IO])

      val response = deleteHook(projectHookId.projectId, authUser).unsafeRunSync()

      response.status                   shouldBe Ok
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe json"""{"message": "Hook deleted"}"""

    }

    "return NOT_FOUND when hook was already deleted" in new TestCase {

      (hookDeletor
        .deleteHook(_: HookIdentifier, _: AccessToken))
        .expects(projectHookId, authUser.accessToken)
        .returning(HookNotFound.pure[IO])

      val response = deleteHook(projectHookId.projectId, authUser).unsafeRunSync()

      response.status                   shouldBe NotFound
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe json"""{"message": "Hook already deleted"}"""
    }

    "return INTERNAL_SERVER_ERROR when there was an error during hook deletion and log the error" in new TestCase {

      val errorMessage = ErrorMessage("some error")
      val exception    = new Exception(errorMessage.toString())
      (hookDeletor
        .deleteHook(_: HookIdentifier, _: AccessToken))
        .expects(projectHookId, authUser.accessToken)
        .returning(IO.raiseError(exception))

      val response = deleteHook(projectHookId.projectId, authUser).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe errorMessage.asJson

      logger.loggedOnly(Error(exception.getMessage, exception))
    }

    "return UNAUTHORIZED when there was an UnauthorizedException thrown during hook creation" in new TestCase {

      (hookDeletor
        .deleteHook(_: HookIdentifier, _: AccessToken))
        .expects(projectHookId, authUser.accessToken)
        .returning(IO.raiseError(UnauthorizedException))

      val response = deleteHook(projectHookId.projectId, authUser).unsafeRunSync()

      response.status                   shouldBe Unauthorized
      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(UnauthorizedException).asJson
    }
  }

  private trait TestCase {

    val projectHookId = projectHookIds.generateOne
    val authUser      = authUsers.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val hookDeletor = mock[HookDeletor[IO]]

    val deleteHook = new HookDeletionEndpointImpl[IO](projectHookId.projectHookUrl, hookDeletor).deleteHook _
  }
}
