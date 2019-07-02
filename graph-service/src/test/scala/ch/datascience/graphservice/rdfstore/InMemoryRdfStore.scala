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

import java.io.ByteArrayInputStream

import cats.effect.{IO, Resource}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graphservice.rdfstore.RDFStoreConfig.FusekiBaseUrl
import ch.datascience.graphservice.rdfstore.RDFStoreGenerators._
import org.apache.jena.fuseki.main.FusekiServer
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdfconnection.{RDFConnection, RDFConnectionFuseki}
import org.http4s.Uri
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Suite}

trait InMemoryRdfStore extends BeforeAndAfterAll with BeforeAndAfter {
  this: Suite =>

  private val fusekiServerPort = 3030
  private val rdfStoreConfig = rdfStoreConfigs.generateOne.copy(
    fusekiBaseUrl = FusekiBaseUrl(s"http://localhost:$fusekiServerPort")
  )
  import rdfStoreConfig._
  private lazy val renkuDataSet = DatasetFactory.createTxnMem()
  private lazy val rdfStoreServer: FusekiServer = FusekiServer
    .create()
    .loopback(true)
    .port(fusekiServerPort)
    .add(s"/$datasetName", renkuDataSet)
    .build

  protected val sparqlEndpoint: Uri = Uri
    .fromString(s"$fusekiBaseUrl/$datasetName/sparql")
    .fold(throw _, identity)

  private val rdfConnectionResource: Resource[IO, RDFConnection] = Resource
    .make(openConnection)(connection => IO(connection.close()))

  private def openConnection: IO[RDFConnection] = IO {
    RDFConnectionFuseki
      .create()
      .destination((fusekiBaseUrl / datasetName).toString)
      .build()
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    rdfStoreServer.start()
    ()
  }

  before {
    renkuDataSet.asDatasetGraph().clear()
    renkuDataSet.asDatasetGraph().isEmpty shouldBe true
  }

  protected override def afterAll(): Unit = {
    rdfStoreServer.stop()
    super.afterAll()
  }

  protected def loadToStore(triples: String): Unit =
    rdfConnectionResource
      .use { connection =>
        IO {
          connection.load {
            ModelFactory.createDefaultModel.read(new ByteArrayInputStream(triples.getBytes), "")
          }
        }
      }
      .unsafeRunSync()
}
