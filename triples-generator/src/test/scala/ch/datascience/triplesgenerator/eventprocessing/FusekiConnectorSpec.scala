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

package ch.datascience.triplesgenerator.eventprocessing

import cats.effect.{ContextShift, IO}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.client.BasicAuthCredentials
import ch.datascience.triplesgenerator.config.FusekiBaseUrl
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdfconnection.RDFConnection
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext
import scala.language.postfixOps

class FusekiConnectorSpec extends WordSpec with MockFactory {

  "upload" should {

    "succeed if upload the triples to Jena Fuseki is successful" in new TestCase {

      createConnection
        .expects(fusekiConfig.fusekiBaseUrl / fusekiConfig.datasetName, fusekiConfig.authCredentials)
        .returning(fusekiConnection)

      (fusekiConnection
        .load(_: Model))
        .expects(rdfTriples.value)

      (fusekiConnection.close _)
        .expects()

      fusekiConnector.upload(rdfTriples).unsafeRunSync() shouldBe ((): Unit)
    }

    "fail if creation connection to Jena fails" in new TestCase {

      val exception = exceptions.generateOne
      createConnection
        .expects(fusekiConfig.fusekiBaseUrl / fusekiConfig.datasetName, fusekiConfig.authCredentials)
        .throwing(exception)

      val actual = intercept[Exception] {
        fusekiConnector.upload(rdfTriples).unsafeRunSync()
      }
      actual.getMessage shouldBe "Uploading triples to Jena failed"
      actual.getCause   shouldBe exception
    }

    "fail if upload to Jena Fuseki fails" in new TestCase {

      createConnection
        .expects(fusekiConfig.fusekiBaseUrl / fusekiConfig.datasetName, fusekiConfig.authCredentials)
        .returning(fusekiConnection)

      val exception = exceptions.generateOne
      (fusekiConnection
        .load(_: Model))
        .expects(rdfTriples.value)
        .throwing(exception)

      (fusekiConnection.close _)
        .expects()

      val actual = intercept[Exception] {
        fusekiConnector.upload(rdfTriples).unsafeRunSync()
      }
      actual.getMessage shouldBe "Uploading triples to Jena failed"
      actual.getCause   shouldBe exception
    }

    "fail if closing the connection to fuseki fails" in new TestCase {

      createConnection
        .expects(fusekiConfig.fusekiBaseUrl / fusekiConfig.datasetName, fusekiConfig.authCredentials)
        .returning(fusekiConnection)

      (fusekiConnection
        .load(_: Model))
        .expects(rdfTriples.value)

      val exception = exceptions.generateOne
      (fusekiConnection.close _)
        .expects()
        .throwing(exception)

      val actual = intercept[Exception] {
        fusekiConnector.upload(rdfTriples).unsafeRunSync()
      }
      actual.getMessage shouldBe "Uploading triples to Jena failed"
      actual.getCause   shouldBe exception
    }
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private trait TestCase {

    val rdfTriples   = rdfTriplesSets.generateOne
    val fusekiConfig = fusekiUserConfigs.generateOne

    val createConnection = mockFunction[FusekiBaseUrl, BasicAuthCredentials, RDFConnection]
    val fusekiConnection = mock[RDFConnection]

    val fusekiConnector = new IOFusekiConnector(fusekiConfig, createConnection)
  }
}
