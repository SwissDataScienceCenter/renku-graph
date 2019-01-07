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
import cats.effect.{ ContextShift, IO }
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events._
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.hookcreation.HookCreationRequestSender.UnauthorizedException
import ch.datascience.webhookservice.model.UserAuthToken
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.play.guice.GuiceOneAppPerTest
import play.api.http.MimeTypes.JSON
import play.api.libs.json.JsValue
import play.api.mvc.ControllerComponents
import play.api.test.Helpers._
import play.api.test.{ FakeRequest, Injecting }

import scala.concurrent.ExecutionContext.Implicits.global

class HookCreationEndpointSpec extends WordSpec with MockFactory with GuiceOneAppPerTest with Injecting {

  "POST /projects/:id/hooks" should {

    "return CREATED when a valid PRIVATE-TOKEN is present in the header " +
      "and webhook is successfully created for project with the given id in in GitLab" in new TestCase {

        ( hookCreator.createHook( _: ProjectId, _: UserAuthToken )( _: MonadError[IO, Throwable] ) )
          .expects( projectId, authToken, * )
          .returning( IO.pure( () ) )

        val response = call( createHook( projectId ), request.withHeaders( "PRIVATE-TOKEN" -> authToken.toString ) )

        status( response ) shouldBe CREATED
        contentAsString( response ) shouldBe ""
      }

    "return UNAUTHORIZED when user PRIVATE-TOKEN is not present" in new TestCase {

      val response = call( createHook( projectId ), request )

      status( response ) shouldBe UNAUTHORIZED
      contentType( response ) shouldBe Some( JSON )
      contentAsJson( response ) shouldBe a[JsValue]
    }

    "return UNAUTHORIZED when user PRIVATE-TOKEN is invalid" in new TestCase {

      val response = call( createHook( projectId ), request.withHeaders( "PRIVATE-TOKEN" -> "" ) )

      status( response ) shouldBe UNAUTHORIZED
      contentType( response ) shouldBe Some( JSON )
      contentAsJson( response ) shouldBe a[JsValue]
    }

    "return BAD_GATEWAY when there was an error during hook creation" in new TestCase {

      val errorMessage = ErrorMessage( "some error" )
      ( hookCreator.createHook( _: ProjectId, _: UserAuthToken )( _: MonadError[IO, Throwable] ) )
        .expects( projectId, authToken, * )
        .returning( IO.raiseError( new Exception( errorMessage.toString() ) ) )

      val response = call( createHook( projectId ), request.withHeaders( "PRIVATE-TOKEN" -> authToken.toString ) )

      status( response ) shouldBe BAD_GATEWAY
      contentType( response ) shouldBe Some( JSON )
      contentAsJson( response ) shouldBe errorMessage.toJson
    }

    "return UNAUTHORIZED when there was an UnauthorizedExcetion thrown during hook creation" in new TestCase {

      val errorMessage = ErrorMessage( "some error" )
      ( hookCreator.createHook( _: ProjectId, _: UserAuthToken )( _: MonadError[IO, Throwable] ) )
        .expects( projectId, authToken, * )
        .returning( IO.raiseError( UnauthorizedException ) )

      val response = call( createHook( projectId ), request.withHeaders( "PRIVATE-TOKEN" -> authToken.toString ) )

      status( response ) shouldBe UNAUTHORIZED
      contentType( response ) shouldBe Some( JSON )
      contentAsJson( response ) shouldBe a[JsValue]
    }
  }

  private trait TestCase {
    implicit val materializer: Materializer = app.materializer
    implicit val cs: ContextShift[IO] = IO.contextShift( global )

    val request = FakeRequest().withHeaders( CONTENT_TYPE -> JSON )
    val projectId = projectIds.generateOne
    val authToken = userAuthTokens.generateOne

    val hookCreator = mock[IOHookCreator]
    val createHook = new HookCreationEndpoint( inject[ControllerComponents], hookCreator ).createHook _
  }
}
