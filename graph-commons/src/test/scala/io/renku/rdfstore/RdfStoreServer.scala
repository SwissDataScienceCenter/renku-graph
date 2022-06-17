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

import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import org.apache.jena.fuseki.FusekiException
import org.apache.jena.fuseki.main.FusekiServer
import org.apache.jena.query.{Dataset, DatasetFactory}

import java.net.BindException
import scala.concurrent.duration._

object RdfStoreServer extends IOApp {

  val renkuDatasetName:      DatasetName          = DatasetName("renku")
  val migrationsDatasetName: DatasetName          = DatasetName("migrations")
  val fusekiPort:            Int Refined Positive = 3030

  override def run(args: List[String]): IO[ExitCode] = for {
    _ <- new RdfStoreServer(fusekiPort, renkuDatasetName, migrationsDatasetName, muteLogging = false).start
  } yield ExitCode.Success
}

class RdfStoreServer(
    port:                  Int Refined Positive,
    datasetName:           DatasetName,
    migrationsDatasetName: DatasetName,
    muteLogging:           Boolean = true
)(implicit temporal:       Temporal[IO]) {

  if (!muteLogging) {
    import org.apache.jena.atlas.logging.LogCtl
    import org.apache.jena.fuseki.Fuseki._
    import org.slf4j.event.Level._

    LogCtl.setJavaLogging()
    LogCtl.setLevel(requestLogName, INFO.toString)
    LogCtl.setLevel(actionLogName, INFO.toString)
    LogCtl.setLevel(serverLogName, ERROR.toString)
    LogCtl.setLevel(adminLogName, ERROR.toString)
    LogCtl.setLevel("org.eclipse.jetty", ERROR.toString)
  }

  private lazy val renkuDataset = {
    import io.renku.graph.model.Schemas._
    import org.apache.jena.graph.NodeFactory
    import org.apache.jena.query.DatasetFactory
    import org.apache.jena.query.text.{EntityDefinition, TextDatasetFactory, TextIndexConfig}
    import org.apache.lucene.store.MMapDirectory

    import java.nio.file.Files

    val entityDefinition: EntityDefinition = {
      val definition = new EntityDefinition("uri", "name")
      definition.setPrimaryPredicate(NodeFactory.createURI((schema / "name").show))
      definition.set("description", NodeFactory.createURI((schema / "description").show))
      definition.set("slug", NodeFactory.createURI((renku / "slug").show))
      definition.set("projectNamespaces", NodeFactory.createURI((renku / "projectNamespaces").show))
      definition.set("keywords", NodeFactory.createURI((schema / "keywords").show))
      definition
    }

    TextDatasetFactory.createLucene(
      DatasetFactory.create(),
      new MMapDirectory(Files.createTempDirectory("lucene-store-jena")),
      new TextIndexConfig(entityDefinition)
    )
  }

  private lazy val migrationsDataset = DatasetFactory.create()

  private lazy val rdfStoreServer: FusekiServer = FusekiServer
    .create()
    .loopback(true)
    .port(port.value)
    .add(s"/$datasetName", renkuDataset)
    .add(s"/$migrationsDatasetName", migrationsDataset)
    .build

  def start: IO[Unit] =
    IO(rdfStoreServer.start()).void.recoverWith {
      case exception: FusekiException =>
        exception.getCause match {
          case _: BindException =>
            (temporal sleep (1 second)) >> start
          case other =>
            IO.raiseError(new IllegalStateException(s"Cannot start fuseki on http://localhost:$port", other))
        }
      case other: Exception =>
        IO.raiseError(new IllegalStateException(s"Cannot start fuseki on http://localhost:$port", other))
    }

  def stop: IO[Unit] = IO(rdfStoreServer.stop())

  def clearDatasets(): IO[Unit] = for {
    _ <- IO(renkuDataset.asDatasetGraph().clear()) >> validateEmpty(renkuDataset, datasetName)
    _ <- IO(migrationsDataset.asDatasetGraph().clear()) >> validateEmpty(migrationsDataset, migrationsDatasetName)
  } yield ()

  private def validateEmpty(dataset: Dataset, name: DatasetName): IO[Unit] =
    IO(dataset.asDatasetGraph().isEmpty) >>= {
      case true  => IO.unit
      case false => IO.raiseError(new Exception(s"Clearing $name wasn't successful"))
    }
}
