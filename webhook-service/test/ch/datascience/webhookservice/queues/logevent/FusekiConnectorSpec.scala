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

package ch.datascience.webhookservice.queues.logevent

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.webhookservice.config.ServiceUrl
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdfconnection.RDFConnection
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.{ global => executionContext }
import scala.concurrent.duration._
import scala.language.{ implicitConversions, postfixOps }

class FusekiConnectorSpec extends WordSpec with MockFactory with ScalaFutures {

  "upload" should {

    "upload the triples to Jena Fuseki" in new TestCase {

      createConnection
        .expects( fusekiConfig.fusekiBaseUrl / fusekiConfig.datasetName )
        .returning( fusekiConnection )

      ( fusekiConnection.load( _: Model ) )
        .expects( rdfTriples.value )

      ( fusekiConnection.close _ )
        .expects()

      fusekiConnector.upload( rdfTriples ).futureValue
    }

    "return failure if upload to Jena Fuseki fails" in new TestCase {

      createConnection
        .expects( fusekiConfig.fusekiBaseUrl / fusekiConfig.datasetName )
        .returning( fusekiConnection )

      val exception: Exception = new Exception( "message" )
      ( fusekiConnection.load( _: Model ) )
        .expects( rdfTriples.value )
        .throwing( exception )

      ( fusekiConnection.close _ )
        .expects()

      intercept[Exception] {
        Await.result( fusekiConnector.upload( rdfTriples ), 1 second )
      } shouldBe exception
    }

    "return failure if creating an url to fuseki fails" in new TestCase {

      val exception: Exception = new Exception( "message" )
      createConnection
        .expects( fusekiConfig.fusekiBaseUrl / fusekiConfig.datasetName )
        .throwing( exception )

      intercept[Exception] {
        Await.result( fusekiConnector.upload( rdfTriples ), 1 second )
      } shouldBe exception
    }

    "return failure if closing connection to fuseki fails" in new TestCase {

      createConnection
        .expects( fusekiConfig.fusekiBaseUrl / fusekiConfig.datasetName )
        .returning( fusekiConnection )

      ( fusekiConnection.load( _: Model ) )
        .expects( rdfTriples.value )

      val exception: Exception = new Exception( "message" )
      ( fusekiConnection.close _ )
        .expects().twice()
        .throwing( exception )

      intercept[Exception] {
        Await.result( fusekiConnector.upload( rdfTriples ), 1 second )
      } shouldBe exception
    }
  }

  private trait TestCase {

    val rdfTriples: RDFTriples = rdfTriplesSets.generateOne

    val createConnection = mockFunction[ServiceUrl, RDFConnection]
    val fusekiConnection: RDFConnection = mock[RDFConnection]
    val fusekiConfig = fusekiConfigs.generateOne

    val fusekiConnector = new FusekiConnector( fusekiConfig, createConnection )
  }
}
