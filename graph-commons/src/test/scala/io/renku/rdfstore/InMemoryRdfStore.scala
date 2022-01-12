/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.rdfstore

import cats.data.Validated
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.circe.{Decoder, HCursor, Json}
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.http.client.{BasicAuthCredentials, BasicAuthPassword, BasicAuthUsername}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.{JsonLD, JsonLDEncoder}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdfconnection.{RDFConnection, RDFConnectionFuseki}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.http4s.Uri
import org.scalatest._

import java.io.ByteArrayInputStream
import java.net.{ServerSocket, SocketException}
import java.nio.charset.StandardCharsets.UTF_8
import scala.language.reflectiveCalls
import scala.util.Random.nextInt
import scala.xml.Elem

trait InMemoryRdfStore extends BeforeAndAfterAll with BeforeAndAfter {
  this: Suite with IOSpec =>

  protected val givenServerRunning: Boolean = false

  private lazy val fusekiServerPort: Int Refined Positive =
    if (givenServerRunning) 3030
    else
      (Option(nextInt(30000) + 3000) find notUsedPort)
        .map(Refined.unsafeApply[Int, Positive])
        .getOrElse(fusekiServerPort)

  private lazy val notUsedPort: Int => Boolean = { port =>
    Validated
      .catchOnly[SocketException](new ServerSocket(port).close())
      .isValid
  }

  protected lazy val rdfStoreConfig: RdfStoreConfig = rdfStoreConfigs.generateOne.copy(
    fusekiBaseUrl = FusekiBaseUrl(s"http://localhost:$fusekiServerPort"),
    datasetName =
      if (givenServerRunning) DatasetName("renku") else (nonEmptyStrings() map DatasetName.apply).generateOne,
    authCredentials = BasicAuthCredentials(BasicAuthUsername("admin"), BasicAuthPassword("admin"))
  )

  protected implicit lazy val fusekiBaseUrl: FusekiBaseUrl = rdfStoreConfig.fusekiBaseUrl

  private lazy val rdfStoreServer = new RdfStoreServer(fusekiServerPort, rdfStoreConfig.datasetName)

  protected lazy val sparqlEndpoint: Uri = Uri
    .fromString(s"$fusekiBaseUrl/${rdfStoreConfig.datasetName}/sparql")
    .fold(throw _, identity)

  private lazy val rdfConnectionResource: Resource[IO, RDFConnection] =
    Resource.make(openConnection)(connection => IO(connection.close()))

  private def openConnection: IO[RDFConnection] = IO {
    RDFConnectionFuseki
      .create()
      .destination((fusekiBaseUrl / rdfStoreConfig.datasetName).toString)
      .build()
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    if (!givenServerRunning) rdfStoreServer.start.unsafeRunSync()
  }

  before {
    if (!givenServerRunning) {
      clearDataset()
    }
  }

  def clearDataset(): Unit = rdfStoreServer.clearDataset().unsafeRunSync()

  protected override def afterAll(): Unit = {
    if (!givenServerRunning) rdfStoreServer.stop.unsafeRunSync()
    super.afterAll()
  }

  protected def loadToStore(triples: Elem): Unit = rdfConnectionResource
    .use { connection =>
      IO {
        connection.load {
          ModelFactory.createDefaultModel.read(new ByteArrayInputStream(triples.toString().getBytes), "")
        }
      }
    }
    .unsafeRunSync()

  protected def loadToStore(triples: JsonLD): Unit = rdfConnectionResource
    .use { connection =>
      IO {
        connection.load {
          val model = ModelFactory.createDefaultModel()
          RDFDataMgr.read(model, new ByteArrayInputStream(triples.toJson.noSpaces.getBytes(UTF_8)), null, Lang.JSONLD)
          model
        }
      }
    }
    .unsafeRunSync()

  protected def loadToStore(jsonLDs: JsonLD*): Unit = rdfConnectionResource
    .use { connection =>
      IO {
        connection.load {
          val model = ModelFactory.createDefaultModel()

          val flattenedJsonLDs: Seq[JsonLD] =
            jsonLDs.flatMap(_.flatten.toOption.flatMap(_.asArray).getOrElse(List.empty[JsonLD]))
          RDFDataMgr.read(
            model,
            new ByteArrayInputStream(Json.arr(flattenedJsonLDs.map(_.toJson): _*).noSpaces.getBytes(UTF_8)),
            null,
            Lang.JSONLD
          )
          model
        }
      }
    }
    .unsafeRunSync()

  protected def loadToStore[T](objects: T*)(implicit encoder: JsonLDEncoder[T]): Unit = loadToStore(
    objects.map(encoder.apply): _*
  )

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private lazy val queryRunner = new RdfStoreClientImpl(
    rdfStoreConfig,
    new SparqlQueryTimeRecorder(TestExecutionTimeRecorder[IO]())
  ) {

    import io.circe.Decoder._

    def runQuery(query: String): IO[List[Map[String, String]]] =
      queryExpecting[List[Map[String, String]]] {
        SparqlQuery(
          name = "test query",
          Set(
            "PREFIX prov: <http://www.w3.org/ns/prov#>",
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
            "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
            "PREFIX wfdesc: <http://purl.org/wf4ever/wfdesc#>",
            "PREFIX wf: <http://www.w3.org/2005/01/wf/flow#>",
            "PREFIX wfprov: <http://purl.org/wf4ever/wfprov#>",
            "PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
            "PREFIX schema: <http://schema.org/>"
          ),
          query
        )
      }

    def runUpdate(query: SparqlQuery): IO[Unit] = updateWithNoResult(using = query)

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

  protected def runQuery(query: String): IO[List[Map[String, String]]] = queryRunner.runQuery(query)

  protected def runUpdate(query: SparqlQuery): IO[Unit] = queryRunner.runUpdate(query)

  protected implicit class UpdatesRunner(updates: List[SparqlQuery]) {
    lazy val runAll: IO[Unit] = (updates map runUpdate).sequence.void
  }

  protected def rdfStoreSize: Int =
    runQuery("SELECT (COUNT(*) as ?triples) WHERE { ?s ?p ?o }")
      .unsafeRunSync()
      .map(row => row("triples"))
      .headOption
      .map(_.toInt)
      .getOrElse(throw new Exception("Cannot find the count of all the triples"))
}
