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

package ch.datascience.rdfstore

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

import java.io.ByteArrayInputStream
import java.net.{ServerSocket, SocketException}
import java.nio.charset.StandardCharsets.UTF_8

import cats.MonadError
import cats.data.Validated
import cats.effect._
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.IORdfStoreClient.RdfQuery
import io.circe.{Decoder, HCursor, Json}
import org.apache.jena.fuseki.main.FusekiServer
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdfconnection.{RDFConnection, RDFConnectionFuseki}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.http4s.Uri
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Suite}

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global
import scala.language.reflectiveCalls
import scala.util.Random.shuffle
import scala.xml.Elem

trait InMemoryRdfStore extends BeforeAndAfterAll with BeforeAndAfter {
  this: Suite =>

  protected implicit val ec:    ExecutionContext          = global
  protected implicit val cs:    ContextShift[IO]          = IO.contextShift(global)
  protected implicit val timer: Timer[IO]                 = IO.timer(global)
  protected val context:        MonadError[IO, Throwable] = MonadError[IO, Throwable]

  private val fusekiServerPort = (shuffle((3000 to 3500).toList) find notUsedPort)
    .getOrElse(throw new Exception("Cannot find not used port for Fuseki"))

  private lazy val notUsedPort: Int => Boolean = { port =>
    Validated
      .catchOnly[SocketException] { new ServerSocket(port).close() }
      .isValid
  }

  protected val rdfStoreConfig: RdfStoreConfig = rdfStoreConfigs.generateOne.copy(
    fusekiBaseUrl = FusekiBaseUrl(s"http://localhost:$fusekiServerPort")
  )
  protected implicit lazy val fusekiBaseUrl: FusekiBaseUrl = rdfStoreConfig.fusekiBaseUrl
  import rdfStoreConfig.datasetName
  private lazy val renkuDataset = DatasetFactory.createTxnMem()
  private lazy val rdfStoreServer: FusekiServer = FusekiServer
    .create()
    .loopback(true)
    .port(fusekiServerPort)
    .add(s"/$datasetName", renkuDataset)
    .build

  protected val sparqlEndpoint: Uri = Uri
    .fromString(s"$fusekiBaseUrl/$datasetName/sparql")
    .fold(throw _, identity)

  private val rdfConnectionResource: Resource[IO, RDFConnection] =
    Resource.make(openConnection)(connection => IO(connection.close()))

  private def openConnection: IO[RDFConnection] = IO {
    RDFConnectionFuseki
      .create()
      .destination((fusekiBaseUrl / datasetName).toString)
      .build()
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    IO(rdfStoreServer.start())
      .recover {
        case exception =>
          throw new IllegalStateException(s"Cannot start fuseki on $fusekiBaseUrl", exception)
      }
      .unsafeRunSync()
    ()
  }

  before {
    renkuDataset.asDatasetGraph().clear()
    renkuDataset.asDatasetGraph().isEmpty shouldBe true
  }

  protected override def afterAll(): Unit = {
    rdfStoreServer.stop()
    super.afterAll()
  }

  protected def loadToStore(triples: Elem): Unit =
    rdfConnectionResource
      .use { connection =>
        IO {
          connection.load {
            ModelFactory.createDefaultModel.read(new ByteArrayInputStream(triples.toString().getBytes), "")
          }
        }
      }
      .unsafeRunSync()

  protected def loadToStore(triples: JsonLDTriples): Unit =
    rdfConnectionResource
      .use { connection =>
        IO {
          connection.load {
            val model = ModelFactory.createDefaultModel()
            RDFDataMgr.read(model, new ByteArrayInputStream(triples.value.noSpaces.getBytes(UTF_8)), null, Lang.JSONLD)
            model
          }
        }
      }
      .unsafeRunSync()

  private val queryRunner = new IORdfStoreClient[RdfQuery](rdfStoreConfig, TestLogger()) {

    import cats.implicits._
    import io.circe.Decoder._

    def run(query: String): IO[List[Map[String, String]]] =
      queryExpecting[List[Map[String, String]]] {
        s"""
           |PREFIX prov: <http://www.w3.org/ns/prov#>
           |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
           |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
           |PREFIX wfdesc: <http://purl.org/wf4ever/wfdesc#>
           |PREFIX wf: <http://www.w3.org/2005/01/wf/flow#>
           |PREFIX wfprov: <http://purl.org/wf4ever/wfprov#>
           |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
           |PREFIX schema: <http://schema.org/>
           |PREFIX dcterms: <http://purl.org/dc/terms/>
           |
           |$query""".stripMargin
      }

    private implicit lazy val valuesDecoder: Decoder[List[Map[String, String]]] = { cursor =>
      for {
        vars <- cursor.as[List[String]]
        values <- cursor
                   .downField("results")
                   .downField("bindings")
                   .as[List[Map[String, String]]](decodeList(valuesDecoder(vars)))
      } yield values
    }

    private implicit lazy val varsDecoder: Decoder[List[String]] =
      _.downField("head").downField("vars").as[List[Json]].flatMap(_.map(_.as[String]).sequence)

    private def valuesDecoder(vars: List[String]): Decoder[Map[String, String]] =
      implicit cursor =>
        vars
          .map(varToMaybeValue)
          .sequence
          .map(_.flatten)
          .map(_.toMap)

    private def varToMaybeValue(varName: String)(implicit cursor: HCursor) =
      cursor
        .downField(varName)
        .downField("value")
        .as[Option[String]]
        .map(maybeValue => maybeValue map (varName -> _))
  }

  protected def runQuery(query: String): IO[List[Map[String, String]]] = queryRunner.run(query)

  protected def rdfStoreSize: Int =
    runQuery("SELECT (COUNT(*) as ?triples) WHERE { ?s ?p ?o }")
      .unsafeRunSync()
      .map(row => row("triples"))
      .headOption
      .map(_.toInt)
      .getOrElse(throw new Exception("Cannot find the count of all the triples"))
}
