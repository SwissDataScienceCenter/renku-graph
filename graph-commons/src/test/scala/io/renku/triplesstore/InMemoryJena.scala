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

package io.renku.triplesstore

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.syntax.all._
import com.dimafeng.testcontainers.{FixedHostPortGenericContainer, GenericContainer, SingleContainer}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.circe.{Decoder, HCursor, Json}
import io.renku.graph.triplesstore.DatasetTTLs._
import io.renku.http.client._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.{JsonLD, JsonLDEncoder}
import io.renku.logging.TestSparqlQueryTimeRecorder
import org.testcontainers.containers.wait.strategy.Wait

import scala.collection.mutable
import scala.language.reflectiveCalls

trait InMemoryJena {

  protected val maybeJenaFixedPort: Option[Int Refined Positive] = None

  private val adminCredentials = BasicAuthCredentials(BasicAuthUsername("admin"), BasicAuthPassword("admin"))

  lazy val container: SingleContainer[_] = maybeJenaFixedPort match {
    case None =>
      GenericContainer(
        dockerImage = "renku/renku-jena:0.0.14",
        exposedPorts = Seq(3030),
        waitStrategy = Wait forHttp "/$/ping"
      )
    case Some(fixedPort) =>
      FixedHostPortGenericContainer(
        imageName = "renku/renku-jena:0.0.14",
        exposedPorts = Seq(3030),
        exposedHostPort = fixedPort.value,
        exposedContainerPort = fixedPort.value,
        waitStrategy = Wait forHttp "/$/ping"
      )
  }

  private lazy val fusekiServerPort: Int Refined Positive = maybeJenaFixedPort match {
    case None       => Refined.unsafeApply(container.mappedPort(container.exposedPorts.head))
    case Some(port) => port
  }

  lazy val fusekiUrl: FusekiUrl = FusekiUrl(s"http://localhost:$fusekiServerPort")

  private val datasets: mutable.Map[FusekiUrl => DatasetConnectionConfig, DatasetConfigFile] = mutable.Map.empty

  protected def registerDataset(connectionInfoFactory: FusekiUrl => DatasetConnectionConfig,
                                maybeConfigFile:       Either[Exception, DatasetConfigFile]
  ): Unit = maybeConfigFile
    .map(configFile => datasets.addOne(connectionInfoFactory -> configFile))
    .fold(throw _, _ => ())

  protected def createDatasets(): IO[Unit] =
    datasets
      .map { case (_, configFile) => datasetsCreator.createDataset(configFile) }
      .toList
      .sequence
      .void

  def clearAllDatasets()(implicit ioRuntime: IORuntime): Unit =
    datasets
      .map { case (connectionInfoFactory, _) => connectionInfoFactory(fusekiUrl).datasetName }
      .foreach(clear)

  def clear(dataset: DatasetName)(implicit ioRuntime: IORuntime): Unit =
    queryRunnerFor(dataset)
      .runUpdate(
        SparqlQuery.of("delete all data", "CLEAR ALL")
      )
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

  def insert(to: DatasetName, triple: Triple)(implicit ioRuntime: IORuntime): Unit = queryRunnerFor(to)
    .runUpdate {
      SparqlQuery.of("insert triple", show"INSERT DATA { $triple }")
    }
    .unsafeRunSync()

  def delete(from: DatasetName, triple: Triple)(implicit ioRuntime: IORuntime): Unit = queryRunnerFor(from)
    .runUpdate {
      SparqlQuery.of("delete triple", show"DELETE DATA { $triple }")
    }
    .unsafeRunSync()

  def runSelect(on: DatasetName, query: SparqlQuery): IO[List[Map[String, String]]] =
    queryRunnerFor(on).runQuery(query)

  def runUpdate(on: DatasetName, query: SparqlQuery): IO[Unit] =
    queryRunnerFor(on).runUpdate(query)

  def triplesCount(on: DatasetName)(implicit ioRuntime: IORuntime): Long =
    queryRunnerFor(on)
      .runQuery(
        SparqlQuery.of("triples count", "SELECT (COUNT(?s) AS ?count) WHERE { ?s ?p ?o }")
      )
      .map(_.headOption.map(_.apply("count")).flatMap(_.toLongOption).getOrElse(0L))
      .unsafeRunSync()

  implicit class QueriesOps(queries: List[SparqlQuery]) {
    def runAll(on: DatasetName): IO[Unit] = {
      val runner = queryRunnerFor(on)
      queries.map(runner.runUpdate).sequence.void
    }
  }

  private def findConnectionInfo(datasetName: DatasetName): DatasetConnectionConfig = datasets
    .map { case (connectionInfoFactory, _) => connectionInfoFactory(fusekiUrl) }
    .find(_.datasetName == datasetName)
    .getOrElse(throw new Exception(s"Dataset '$datasetName' not registered in Test Jena instance"))

  private implicit lazy val logger:  TestLogger[IO]              = TestLogger[IO]()
  private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]

  private lazy val datasetsCreator = RdfStoreAdminClient[IO](AdminConnectionConfig(fusekiUrl, adminCredentials))

  private def queryRunnerFor(datasetName: DatasetName) = queryRunner(findConnectionInfo(datasetName))

  private def queryRunner(connectionInfo: DatasetConnectionConfig) = new RdfStoreClientImpl[IO](connectionInfo) {

    import io.circe.Decoder._

    def uploadPayload(jsonLD: JsonLD) = upload(jsonLD)

    def runQuery(query: SparqlQuery): IO[List[Map[String, String]]] =
      queryExpecting[List[Map[String, String]]](query)

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
}

trait JenaDataset {
  self: InMemoryJena =>
}

trait RenkuDataset extends JenaDataset {
  self: InMemoryJena =>

  private lazy val configFile: Either[Exception, DatasetConfigFile] = RenkuTTL.fromTtlFile()
  private lazy val connectionInfoFactory: FusekiUrl => RenkuConnectionConfig = RenkuConnectionConfig(
    _,
    BasicAuthCredentials(BasicAuthUsername("renku"), BasicAuthPassword("renku"))
  )

  def renkuDataset:          DatasetName           = renkuDSConnectionInfo.datasetName
  def renkuDSConnectionInfo: RenkuConnectionConfig = connectionInfoFactory(fusekiUrl)

  registerDataset(connectionInfoFactory, configFile)
}

trait MigrationsDataset extends JenaDataset {
  self: InMemoryJena =>

  private lazy val configFile: Either[Exception, MigrationsTTL] = MigrationsTTL.fromTtlFile()
  private lazy val connectionInfoFactory: FusekiUrl => MigrationsConnectionConfig = MigrationsConnectionConfig(
    _,
    BasicAuthCredentials(BasicAuthUsername("admin"), BasicAuthPassword("admin"))
  )

  def migrationsDataset:          DatasetName                = migrationsDSConnectionInfo.datasetName
  def migrationsDSConnectionInfo: MigrationsConnectionConfig = connectionInfoFactory(fusekiUrl)

  registerDataset(connectionInfoFactory, configFile)
}
