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

package ch.datascience.triplesgenerator

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators
import org.scalamock.scalatest.MixedMockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import play.api.LoggerLike
import play.api.libs.ws.ahc.AhcWSResponse
import play.api.libs.ws.{ BodyWritable, WSAuthScheme, WSClient, WSRequest }
import play.api.test.Helpers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.language.postfixOps

class FusekiDatasetVerifierSpec extends WordSpec with MixedMockFactory with ScalaFutures with Eventually {

  "assureDatasetExists" should {

    "do nothing if relevant dataset exists" in new TestCase {

      callToCheckDatasetExists
        .returning( OK )

      datasetVerifier.assureDatasetExists.futureValue shouldBe Done

      verifyInfoLogged( s"'${fusekiConfig.datasetName}' dataset exists in Jena; No action needed." )
    }

    "create a dataset if relevant dataset does not exist" in new TestCase {

      callToCheckDatasetExists
        .returning( NOT_FOUND )

      createDataset
        .returning( OK )

      datasetVerifier.assureDatasetExists.futureValue shouldBe Done

      verifyInfoLogged(
        s"'${fusekiConfig.datasetName}' dataset does not exist in Jena.",
        s"'${fusekiConfig.datasetName}' created in Jena."
      )
    }

    "throw an exception if check of dataset existence fails" in new TestCase {
      val exception = new Exception( "message" )
      callToCheckDatasetExists
        .throwing( exception )

      intercept[Exception] {
        Await.result( datasetVerifier.assureDatasetExists, 1 second )
      } shouldBe exception
    }

    "throw an exception if dataset creation fails" in new TestCase {
      callToCheckDatasetExists
        .returning( NOT_FOUND )

      val exception = new Exception( "message" )
      createDataset
        .throwing( exception )

      intercept[Exception] {
        Await.result( datasetVerifier.assureDatasetExists, 1 second )
      } shouldBe exception
    }
  }

  private trait TestCase {
    private implicit val system = ActorSystem()
    private implicit val materializer = ActorMaterializer()

    val fusekiConfig = ServiceTypesGenerators.fusekiConfigs.generateOne
    private val httpClient = mock[WSClient]
    private val logger = Proxy.stub[LoggerLike]
    lazy val datasetVerifier = new FusekiDatasetVerifier( fusekiConfig, httpClient, logger )

    def callToCheckDatasetExists = {

      import fusekiConfig._

      val request = mock[WSRequest]
      val response = mock[AhcWSResponse]

      ( httpClient.url( _: String ) )
        .expects( ( fusekiBaseUrl / "$" / "datasets" / datasetName ).toString )
        .returning( request )

      ( request.withAuth( _: String, _: String, _: WSAuthScheme ) )
        .expects( username.toString, password.toString, WSAuthScheme.BASIC )
        .returning( request )

      ( request.get _ )
        .expects()
        .returning( Future( response ) )

      ( response.status _ )
        .expects()
    }

    def createDataset = {

      import fusekiConfig._

      val request = mock[WSRequest]
      val response = mock[AhcWSResponse]

      ( httpClient.url( _: String ) )
        .expects( ( fusekiBaseUrl / "$" / "datasets" ).toString )
        .returning( request )

      ( request.withHttpHeaders _ )
        .expects( Seq( CONTENT_TYPE -> "application/x-www-form-urlencoded" ) )
        .returning( request )

      ( request.withAuth( _: String, _: String, _: WSAuthScheme ) )
        .expects( username.toString, password.toString, WSAuthScheme.BASIC )
        .returning( request )

      ( request.post[Map[String, String]]( _: Map[String, String] )( _: BodyWritable[Map[String, String]] ) )
        .expects( Map( "dbName" -> datasetName.toString, "dbType" -> datasetType.toString ), * )
        .returning( Future.successful( response ) )

      ( response.status _ )
        .expects()
    }

    def verifyInfoLogged( expectedMessages: String* ) = {
      var callsCounter = 0
      logger
        .verify( 'info )(
          argAssert { ( message: () => String ) =>
            message() shouldBe expectedMessages( callsCounter )
            callsCounter = callsCounter + 1
          }, *
        )
        .repeat( expectedMessages.size )
    }
  }
}
