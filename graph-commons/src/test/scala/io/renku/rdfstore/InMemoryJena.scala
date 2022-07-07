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

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.syntax.all._
import com.dimafeng.testcontainers.GenericContainer
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.circe.{Decoder, HCursor, Json}
import io.renku.graph.model.views.RdfResource
import io.renku.graph.rdfstore.DatasetTTLs._
import io.renku.http.client._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.{EntityId, JsonLD, JsonLDEncoder}
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.tinytypes.Renderer
import org.testcontainers.containers.wait.strategy.Wait

import scala.collection.mutable
import scala.language.reflectiveCalls

trait InMemoryJena {

  protected val givenServerRunning: Boolean = false

  private val adminCredentials = BasicAuthCredentials(BasicAuthUsername("admin"), BasicAuthPassword("admin"))

  val container: GenericContainer = GenericContainer(
    dockerImage = "renku/renku-jena:0.0.7",
    exposedPorts = Seq(3030),
    waitStrategy = Wait forHttp "/$/ping"
  )

  private lazy val fusekiServerPort: Int Refined Positive = Refined.unsafeApply {
    if (givenServerRunning) 3030
    else container.container.getMappedPort(container.exposedPorts.head)
  }

  protected lazy val fusekiUrl: FusekiBaseUrl = FusekiBaseUrl(s"http://localhost:$fusekiServerPort")

  private val datasets: mutable.Map[FusekiBaseUrl => TriplesStoreConfig, DatasetConfigFile] = mutable.Map.empty

  protected def registerDataset(connectionInfoFactory: FusekiBaseUrl => TriplesStoreConfig,
                                maybeConfigFile:       Either[Exception, DatasetConfigFile]
  ): Unit = maybeConfigFile
    .map(configFile => datasets.addOne(connectionInfoFactory -> configFile))
    .fold(throw _, _ => ())

  protected def createDatasets()(implicit ioRuntime: IORuntime): Unit =
    datasets
      .map { case (connectionInfoFactory, configFile) =>
        queryRunner(connectionInfoFactory(fusekiUrl)).createDataset(configFile, adminCredentials)
      }
      .toList
      .sequence
      .void
      .unsafeRunSync()

  def clear(dataset: DatasetName)(implicit ioRuntime: IORuntime): Unit = queryRunnerFor(dataset)
    .runUpdate("DROP GRAPH ALL")
    .unsafeRunSync()

  def upload(to: DatasetName, jsonLDs: JsonLD*)(implicit ioRuntime: IORuntime): Unit = {
    val jsonLD = JsonLD.arr(jsonLDs.flatMap(_.flatten.toOption.flatMap(_.asArray).getOrElse(List.empty[JsonLD])): _*)
    upload(to, jsonLD)
  }

  private def upload(to: DatasetName, jsonLD: JsonLD)(implicit ioRuntime: IORuntime): Unit =
    queryRunnerFor(to)
      .uploadPayload(jsonLD)
      .unsafeRunSync()

  def upload[T](to: DatasetName, objects: T*)(implicit encoder: JsonLDEncoder[T], ioRuntime: IORuntime): Unit =
    upload(to, objects.map(encoder.apply): _*)

  def insertTriple(to: DatasetName, entityId: EntityId, p: String, o: String)(implicit
      ioRuntime:       IORuntime
  ): Unit = queryRunnerFor(to)
    .runUpdate {
      show"INSERT DATA { <$entityId> $p $o }"
    }
    .unsafeRunSync()

  def insertTriple[R](to: DatasetName, entityId: R, p: String, o: String)(implicit
      entityIdRenderer:   Renderer[RdfResource, R],
      ioRuntime:          IORuntime
  ): Unit = queryRunnerFor(to)
    .runUpdate {
      show"INSERT DATA { ${entityIdRenderer.render(entityId)} $p $o }"
    }
    .unsafeRunSync()

  def deleteTriple(from: DatasetName, entityId: EntityId, p: String, o: String)(implicit
      ioRuntime:         IORuntime
  ): Unit = queryRunnerFor(from)
    .runUpdate {
      show"DELETE DATA { <$entityId> $p $o }"
    }
    .unsafeRunSync()

  private def findConnectionInfo(datasetName: DatasetName): TriplesStoreConfig = datasets
    .map { case (connectionInfoFactory, _) => connectionInfoFactory(fusekiUrl) }
    .find(_.datasetName == datasetName)
    .getOrElse(throw new Exception(s"Dataset '$datasetName' not registered in Test Jena instance"))

  private implicit lazy val logger:  TestLogger[IO]              = TestLogger[IO]()
  private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]

  private def queryRunnerFor(datasetName: DatasetName) = queryRunner(findConnectionInfo(datasetName))

  private def queryRunner(connectionInfo: TriplesStoreConfig) = new RdfStoreClientImpl[IO](connectionInfo) {

    import io.circe.Decoder._
    import io.renku.graph.model.Schemas._
    import org.http4s.Method.POST
    import org.http4s.Status.{Conflict, Ok}
    import org.http4s.Uri
    import org.http4s.headers.`Content-Type`

    def createDataset(configFile: DatasetConfigFile, adminCredentials: BasicAuthCredentials) = for {
      uri <- validateUri(s"$fusekiUrl/$$/datasets")
      request = createDatasetRequest(uri, configFile, adminCredentials)
      uploadResult <- send(request) { case (Ok | Conflict, _, _) => ().pure[IO] }
    } yield uploadResult

    private def createDatasetRequest(uri: Uri, configFile: DatasetConfigFile, credentials: BasicAuthCredentials) =
      HttpRequest(
        request(POST, uri, credentials)
          .withEntity(configFile.show)
          .putHeaders(`Content-Type`(`text/turtle`)),
        name = "dataset creation"
      )

    def uploadPayload(jsonLD: JsonLD) = upload(jsonLD)

    def runQuery(query: SparqlQuery): IO[List[Map[String, String]]] =
      queryExpecting[List[Map[String, String]]](query)

    def runQuery(query: String): IO[List[Map[String, String]]] =
      queryExpecting[List[Map[String, String]]] {
        SparqlQuery.of(
          name = "test query",
          Prefixes.of(
            prov   -> "prov",
            rdf    -> "rdf",
            rdfs   -> "rdfs",
            renku  -> "renku",
            schema -> "schema",
            xsd    -> "xsd"
          ),
          query
        )
      }

    def runUpdate(query: SparqlQuery): IO[Unit] = updateWithNoResult(using = query)

    def runUpdate(query: String): IO[Unit] = runUpdate(
      SparqlQuery.of(
        name = "test query",
        Prefixes.of(
          prov   -> "prov",
          rdf    -> "rdf",
          rdfs   -> "rdfs",
          renku  -> "renku",
          schema -> "schema",
          xsd    -> "xsd"
        ),
        query
      )
    )

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
}

trait JenaDataset[C <: TriplesStoreConfig] {
  self: InMemoryJena =>

  protected def configFile:            Either[Exception, DatasetConfigFile]
  protected def connectionInfoFactory: FusekiBaseUrl => C

  registerDataset(connectionInfoFactory, configFile)
}

trait RenkuDataset extends JenaDataset[RdfStoreConfig] {
  self: InMemoryJena =>

  protected lazy val configFile: Either[Exception, DatasetConfigFile] = RenkuTTL.fromConfigMap()
  protected lazy val connectionInfoFactory: FusekiBaseUrl => RdfStoreConfig = RdfStoreConfig(
    _,
    BasicAuthCredentials(BasicAuthUsername("renku"), BasicAuthPassword("renku"))
  )
  def renkuDataset: DatasetName = renkuDSConnectionInfo.datasetName

  def renkuDSConnectionInfo: RdfStoreConfig = connectionInfoFactory(fusekiUrl)
}

trait MigrationsDataset extends JenaDataset[MigrationsStoreConfig] {
  self: InMemoryJena =>

  protected lazy val configFile: Either[Exception, MigrationsTTL] = MigrationsTTL.fromConfigMap()
  protected lazy val connectionInfoFactory: FusekiBaseUrl => MigrationsStoreConfig = MigrationsStoreConfig(
    _,
    BasicAuthCredentials(BasicAuthUsername("admin"), BasicAuthPassword("admin"))
  )

  def migrationsDSConnectionInfo: MigrationsStoreConfig = connectionInfoFactory(fusekiUrl)
}
