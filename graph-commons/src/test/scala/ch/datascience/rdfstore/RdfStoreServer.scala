/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import java.net.BindException

import cats.effect._
import cats.implicits._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import org.apache.jena.fuseki.FusekiException
import org.apache.jena.fuseki.main.FusekiServer

import scala.concurrent.duration._
import scala.language.postfixOps

object RdfStoreServer extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    for {
      _ <- new RdfStoreServer(3030, DatasetName("renku")).start
    } yield ExitCode.Success
}

class RdfStoreServer(port: Int Refined Positive, datasetName: DatasetName)(implicit timer: Timer[IO]) {

  private lazy val dataset = {
    import java.nio.file.Files

    import org.apache.jena.graph.NodeFactory
    import org.apache.jena.query.DatasetFactory
    import org.apache.jena.query.text.{EntityDefinition, TextDatasetFactory, TextIndexConfig}
    import org.apache.lucene.store.MMapDirectory

    val entityDefinition: EntityDefinition = {
      val definition = new EntityDefinition("uri", "name")
      definition.setPrimaryPredicate(NodeFactory.createURI("http://schema.org/name"))
      definition.set("description", NodeFactory.createURI("http://schema.org/description"))
      definition
    }

    TextDatasetFactory.createLucene(
      DatasetFactory.createTxnMem(),
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
