package ch.datascience.webhookservice.queue

import java.net.URL

import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{StringValue, TinyType}
import ch.datascience.webhookservice.queue.FusekiConnector.{DatasetName, FusekiUrl}
import com.typesafe.config.Config
import org.apache.jena.rdfconnection.{RDFConnection, RDFConnectionFuseki}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

private class FusekiConnector(fusekiBaseUrl: FusekiUrl,
                              datasetName: DatasetName,
                              fusekiConnectionBuilder: FusekiUrl => RDFConnection) {

  def uploadFile(triplesFile: TriplesFile)
                (implicit executionContext: ExecutionContext): Future[Unit] = Future {
    var connection = Option.empty[RDFConnection]
    Try {
      connection = Some(fusekiConnectionBuilder(fusekiBaseUrl / datasetName))
      connection foreach { conn =>
        conn.load(triplesFile.value)
        conn.close()
      }
    } match {
      case Success(_)                   => ()
      case Failure(NonFatal(exception)) =>
        connection foreach (_.close())
        throw exception
    }
  }
}

private object FusekiConnector {

  import ch.datascience.config.ConfigOps.Implicits._

  case class DatasetName(value: String) extends StringValue with NonBlank

  object DatasetName {

    implicit object DatasetNameFinder extends (Config => String => DatasetName) {
      override def apply(config: Config): String => DatasetName = key => DatasetName(config.getString(key))
    }
  }

  case class FusekiUrl(value: URL) extends TinyType[URL]

  object FusekiUrl {

    def apply(url: String): FusekiUrl = FusekiUrl(new URL(url))

    implicit object FusekiUrlFinder extends (Config => String => FusekiUrl) {
      override def apply(config: Config): String => FusekiUrl = key => FusekiUrl(new URL(config.getString(key)))
    }

    implicit class FusekiUrlOps(fusekiUrl: FusekiUrl) {
      def /(value: Any): FusekiUrl = FusekiUrl(
        new URL(s"$fusekiUrl/$value")
      )
    }
  }

  def apply(config: Config): FusekiConnector = {
    val datasetName = config.get[DatasetName]("services.fuseki.dataset-name")
    val fusekiBaseUrl = config.get[FusekiUrl]("services.fuseki.url")
    val connectionBuilder: FusekiUrl => RDFConnection =
      fusekiUrl =>
        RDFConnectionFuseki
          .create()
          .destination(fusekiUrl.value.toString)
          .build()

    new FusekiConnector(fusekiBaseUrl, datasetName, connectionBuilder)
  }
}