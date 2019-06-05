package ch.datascience.graphservice.rdfstore

import cats.MonadError
import cats.effect.IO
import ch.datascience.graphservice.rdfstore.RDFStoreConfig.FusekiBaseUrl
import org.apache.jena.rdfconnection.{RDFConnection, RDFConnectionFuseki}

import scala.language.higherKinds
import scala.util.control.NonFatal

abstract class RDFConnectionResource[Interpretation[_]] {
  def use[Out](function: RDFConnection => Interpretation[Out]): Interpretation[Out]
}

class IORDFConnectionResource private (
    rdfStoreConfig:          RDFStoreConfig,
    fusekiConnectionBuilder: FusekiBaseUrl => RDFConnection = IORDFConnectionResource.fusekiConnectionBuilder
)(implicit ME:               MonadError[IO, Throwable])
    extends RDFConnectionResource[IO] {

  import cats.implicits._
  import rdfStoreConfig._

  def use[Out](function: RDFConnection => IO[Out]): IO[Out] =
    newConnection
      .bracket(function)(closeConnection)
      .recoverWith(meaningfulError)

  private def newConnection: IO[RDFConnection] = IO {
    fusekiConnectionBuilder(fusekiBaseUrl / datasetName)
  }

  private def closeConnection(connection: RDFConnection): IO[Unit] = IO {
    connection.close()
  }

  private def meaningfulError[Out]: PartialFunction[Throwable, IO[Out]] = {
    case NonFatal(exception) => ME.raiseError(new RuntimeException("RDF Store cannot be accessed", exception))
  }
}

object IORDFConnectionResource {

  private val fusekiConnectionBuilder: FusekiBaseUrl => RDFConnection = fusekiUrl =>
    RDFConnectionFuseki
      .create()
      .destination(fusekiUrl.toString)
      .build()

  def apply(): IO[IORDFConnectionResource] = RDFStoreConfig[IO]() map (new IORDFConnectionResource(_))
}
