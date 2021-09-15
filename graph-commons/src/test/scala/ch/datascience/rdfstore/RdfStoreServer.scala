/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import org.apache.jena.fuseki.FusekiException
import org.apache.jena.fuseki.main.FusekiServer

import java.net.BindException
import scala.concurrent.duration._
import scala.language.postfixOps

object RdfStoreServer extends IOApp {

  val datasetName: DatasetName          = DatasetName("renku")
  val fusekiPort:  Int Refined Positive = 3030

  override def run(args: List[String]): IO[ExitCode] = for {
    _ <- new RdfStoreServer(fusekiPort, datasetName, muteLogging = false).start
  } yield ExitCode.Success
}

class RdfStoreServer(
    port:         Int Refined Positive,
    datasetName:  DatasetName,
    muteLogging:  Boolean = true
)(implicit timer: Timer[IO]) {

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

  private lazy val dataset = {
    import ch.datascience.graph.model.Schemas._
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
      definition.set("keywords", NodeFactory.createURI((schema / "keywords").show))
      definition
    }

    TextDatasetFactory.createLucene(
      DatasetFactory.create(),
      new MMapDirectory(Files.createTempDirectory("lucene-store-jena")),
      new TextIndexConfig(entityDefinition)
    )
  }

  private lazy val rdfStoreServer: FusekiServer = FusekiServer
    .create()
    .loopback(true)
    .port(port.value)
    .add(s"/$datasetName", dataset)
    .build

  def start: IO[Unit] =
    IO(rdfStoreServer.start())
      .map(_ => ())
      .recoverWith {
        case exception: FusekiException =>
          exception.getCause match {
            case _: BindException =>
              (timer sleep (1 second)) flatMap (_ => start)
            case other =>
              IO.raiseError(new IllegalStateException(s"Cannot start fuseki on http://localhost:$port", other))
          }
        case other: Exception =>
          IO.raiseError(new IllegalStateException(s"Cannot start fuseki on http://localhost:$port", other))
      }

  def stop: IO[Unit] = IO(rdfStoreServer.stop())

  def clearDataset(): IO[Unit] =
    for {
      _ <- IO(dataset.asDatasetGraph().clear())
      _ <- isDatasetEmpty flatMap {
             case true  => IO.unit
             case false => IO.raiseError(new Exception(s"Clearing $datasetName wasn't successful"))
           }
    } yield ()

  def isDatasetEmpty: IO[Boolean] = IO {
    dataset.asDatasetGraph().isEmpty
  }
}
