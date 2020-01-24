package ch.datascience.rdfstore

import java.net.BindException

import cats.effect.{ExitCode, IO, IOApp, Timer}
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
    import org.apache.jena.graph.NodeFactory
    import org.apache.jena.query.DatasetFactory
    import org.apache.jena.query.text.{EntityDefinition, TextDatasetFactory, TextIndexConfig}
    import org.apache.lucene.store.RAMDirectory

    val entityDefinition: EntityDefinition = {
      val definition = new EntityDefinition("uri", "name")
      definition.setPrimaryPredicate(NodeFactory.createURI("http://schema.org/name"))
      definition.set("description", NodeFactory.createURI("http://schema.org/description"))
      definition
    }

    TextDatasetFactory.createLucene(
      DatasetFactory.create(),
      new RAMDirectory,
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
              timer.sleep(1 second) *> start
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
      _ <- if (dataset.asDatasetGraph().isEmpty) IO.unit
          else IO.raiseError(new Exception(s"Clearing $datasetName wasn't successful"))
    } yield ()
}
