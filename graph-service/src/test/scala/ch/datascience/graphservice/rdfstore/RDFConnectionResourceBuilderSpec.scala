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

package ch.datascience.graphservice.rdfstore

import RDFStoreGenerators._
import cats.effect.{Bracket, IO, Resource}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graphservice.rdfstore.RDFStoreConfig.FusekiBaseUrl
import org.apache.jena.rdfconnection.RDFConnection
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class RDFConnectionResourceBuilderSpec extends WordSpec with MockFactory {

  "resource" should {

    "create a cats effect Resource" in new TestCase {
      connectionResourceBuilder.resource shouldBe a[Resource[IO, _]]
    }

    "create a Resource which opens an RDF connection to an RDF store from the config" in new TestCase {

      connectionBuilder
        .expects(storeConfig.fusekiBaseUrl / storeConfig.datasetName)
        .returning(rdfConnection)

      (rdfConnection.close _)
        .expects()

      connectionResourceBuilder.resource
        .use { connection =>
          connection shouldBe rdfConnection
          context.unit
        }
        .unsafeRunSync()
    }

    "create a Resource which fails with a meaningful error if connection cannot be established" in new TestCase {

      val exception = exceptions.generateOne
      connectionBuilder
        .expects(storeConfig.fusekiBaseUrl / storeConfig.datasetName)
        .throwing(exception)

      val actualException = intercept[Exception] {
        connectionResourceBuilder.resource
          .use(_ => IO.unit)
          .unsafeRunSync()
      }

      actualException.getMessage shouldBe "RDF Store cannot be accessed"
      actualException.getCause   shouldBe exception
    }
  }

  private trait TestCase {
    implicit val context = Bracket[IO, Throwable]

    val rdfConnection = mock[RDFConnection]

    val storeConfig               = rdfStoreConfigs.generateOne
    val connectionBuilder         = mockFunction[FusekiBaseUrl, RDFConnection]
    val connectionResourceBuilder = new RDFConnectionResourceBuilder[IO](storeConfig, connectionBuilder)
  }
}
