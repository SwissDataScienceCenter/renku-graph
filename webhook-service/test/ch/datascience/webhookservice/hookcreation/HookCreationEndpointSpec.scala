/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.hookcreation

import akka.stream.Materializer
import cats.MonadError
import cats.effect.IO
import ch.datascience.http.client.AccessToken
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events.GraphCommonsGenerators._
import ch.datascience.graph.model.events._
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.hookcreation.HookCreator.HookCreationResult.{HookCreated, HookExisted}
import ch.datascience.webhookservice.security.IOAccessTokenFinder
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.play.guice.GuiceOneAppPerTest
import play.api.http.MimeTypes.JSON
import play.api.mvc.{ControllerComponents, Request}
import play.api.test.Helpers._
import play.api.test.{FakeRequest, Injecting}

class HookCreationEndpointSpec extends WordSpec with MockFactory with GuiceOneAppPerTest with Injecting {

  "POST /projects/:id/webhooks" should {

    "return CREATED when a valid access token is present in the header " +
      "and webhook is successfully created for project with the given id in in GitLab" in new TestCase {

      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: Request[_]))
        .expects(*)
        .returning(context.pure(accessToken))

      (hookCreator
        .createHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(IO.pure(HookCreated))

      val response = call(createHook(projectId), request)

      status(response)          shouldBe CREATED
      contentAsString(response) shouldBe ""
    }

    "return OK when hook was already created" in new TestCase {

      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: Request[_]))
        .expects(*)
        .returning(context.pure(accessToken))

      (hookCreator
        .createHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(IO.pure(HookExisted))

      val response = call(createHook(projectId), request)

      status(response)          shouldBe OK
      contentAsString(response) shouldBe ""
    }

    "return UNAUTHORIZED when finding an access token in the headers fails with UnauthorizedException" in new TestCase {

      (accessTokenFinder
        .findAccessToken(_: Request[_]))
        .expects(*)
        .returning(context.raiseError(UnauthorizedException))

      val response = call(createHook(projectId), request)

      status(response)        shouldBe UNAUTHORIZED
      contentType(response)   shouldBe Some(JSON)
      contentAsJson(response) shouldBe ErrorMessage(UnauthorizedException.getMessage).toJson
    }

    "return INTERNAL_SERVER_ERROR when there was an error during hook creation" in new TestCase {

      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: Request[_]))
        .expects(*)
        .returning(context.pure(accessToken))

      val errorMessage = ErrorMessage("some error")
      (hookCreator
        .createHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(IO.raiseError(new Exception(errorMessage.toString())))

      val response = call(createHook(projectId), request)

      status(response)        shouldBe INTERNAL_SERVER_ERROR
      contentType(response)   shouldBe Some(JSON)
      contentAsJson(response) shouldBe errorMessage.toJson
    }

    "return UNAUTHORIZED when there was an UnauthorizedException thrown during hook creation" in new TestCase {

      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: Request[_]))
        .expects(*)
        .returning(context.pure(accessToken))

      val errorMessage = ErrorMessage("some error")
      (hookCreator
        .createHook(_: ProjectId, _: AccessToken))
        .expects(projectId, accessToken)
        .returning(IO.raiseError(UnauthorizedException))

      val response = call(createHook(projectId), request)

      status(response)        shouldBe UNAUTHORIZED
      contentType(response)   shouldBe Some(JSON)
      contentAsJson(response) shouldBe ErrorMessage(UnauthorizedException.getMessage).toJson
    }
  }

  private trait TestCase {
    implicit val materializer: Materializer = app.materializer
    val context = MonadError[IO, Throwable]

    val request   = FakeRequest().withHeaders(CONTENT_TYPE -> JSON)
    val projectId = projectIds.generateOne

    val hookCreator       = mock[IOHookCreator]
    val accessTokenFinder = mock[IOAccessTokenFinder]
    val createHook        = new HookCreationEndpoint(inject[ControllerComponents], hookCreator, accessTokenFinder).createHook _
  }
}
