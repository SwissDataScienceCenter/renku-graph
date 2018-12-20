/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events._
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.model.GitLabAuthToken
import org.scalamock.scalatest.MixedMockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.play.guice.GuiceOneAppPerTest
import play.api.LoggerLike
import play.api.http.MimeTypes.JSON
import play.api.libs.json.Json.toJson
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.ControllerComponents
import play.api.test.Helpers._
import play.api.test.{FakeRequest, Injecting}

import scala.concurrent.Future

class HookCreationEndpointSpec extends WordSpec with MixedMockFactory with GuiceOneAppPerTest with Injecting {

//  "POST /projects/:id/hooks" should {
//
//    "return CREATED when a webhook is created" in new TestCase {
//
//      ( hookCreator.createHook( _: ProjectId, _: GitLabAuthToken ) )
//        .expects( projectId, authToken )
//        .returning( Future.successful( () ) )
//
//      val response = call( createHook( projectId ), request.withBody( toJson( authToken ) ) )
//
//      status( response ) shouldBe CREATED
//      contentAsString( response ) shouldBe ""
//    }
//
//    "return BAD_REQUEST when auth token is invalid" in new TestCase {
//
//      val response = call( createHook( projectId ), request.withBody( Json.obj() ) )
//
//      status( response ) shouldBe BAD_REQUEST
//      contentType( response ) shouldBe Some( JSON )
//      contentAsJson( response ) shouldBe a[JsValue]
//    }
//
//    "return BAD_GATEWAY when there was an error during hook creation" in new TestCase {
//
//      val errorMessage = ErrorMessage( "some error" )
//      ( hookCreator.createHook( _: ProjectId, _: GitLabAuthToken ) )
//        .expects( projectId, authToken )
//        .returning( Future.failed( new Exception(errorMessage.toString()) ) )
//
//      val response = call( createHook( projectId ), request.withBody( toJson( authToken ) ) )
//
//      status( response ) shouldBe BAD_GATEWAY
//      contentType( response ) shouldBe Some( JSON )
//      contentAsJson( response ) shouldBe errorMessage.toJson
//    }
//  }

  private trait TestCase {
    implicit val materializer: Materializer = app.materializer

    val request = FakeRequest().withHeaders( CONTENT_TYPE -> JSON )
    val projectId = projectIds.generateOne
    val authToken = gitLabAuthTokens.generateOne

    val hookCreator = mock[IOHookCreation]
    val logger = Proxy.stub[LoggerLike]
    val createHook = new HookCreationEndpoint( inject[ControllerComponents], hookCreator ).createHook _
  }
}
