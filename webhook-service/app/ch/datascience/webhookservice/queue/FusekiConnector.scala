/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.queue

import java.net.URL

import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{ StringValue, TinyType }
import com.typesafe.config.Config
import javax.inject.{ Inject, Singleton }
import org.apache.jena.rdfconnection.{ RDFConnection, RDFConnectionFuseki }
import play.api.{ ConfigLoader, Configuration }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

case class DatasetName( value: String ) extends StringValue with NonBlank

object DatasetName {

  implicit object DatasetNameFinder extends ConfigLoader[DatasetName] {
    override def load( config: Config, path: String ): DatasetName = DatasetName( config.getString( path ) )
  }
}

case class FusekiUrl( value: URL ) extends TinyType[URL]

object FusekiUrl {

  def apply( url: String ): FusekiUrl = FusekiUrl( new URL( url ) )

  implicit object FusekiUrlFinder extends ConfigLoader[FusekiUrl] {
    override def load( config: Config, path: String ): FusekiUrl = FusekiUrl( config.getString( path ) )
  }

  implicit class FusekiUrlOps( fusekiUrl: FusekiUrl ) {
    def /( value: Any ): FusekiUrl = FusekiUrl(
      new URL( s"$fusekiUrl/$value" )
    )
  }
}

@Singleton
private class FusekiConnector(
    fusekiBaseUrl:           FusekiUrl,
    datasetName:             DatasetName,
    fusekiConnectionBuilder: FusekiUrl => RDFConnection
) {

  @Inject() def this( configuration: Configuration ) = this(
    configuration.get[FusekiUrl]( "services.fuseki.url" ),
    configuration.get[DatasetName]( "services.fuseki.dataset-name" ),
    ( fusekiUrl: FusekiUrl ) =>
      RDFConnectionFuseki
        .create()
        .destination( fusekiUrl.value.toString )
        .build()
  )

  def uploadFile( triplesFile: TriplesFile )( implicit executionContext: ExecutionContext ): Future[Unit] = Future {
    var connection = Option.empty[RDFConnection]
    Try {
      connection = Some( fusekiConnectionBuilder( fusekiBaseUrl / datasetName ) )
      connection foreach { conn =>
        conn.load( triplesFile.value.toAbsolutePath.toString )
        conn.close()
      }
    } match {
      case Success( _ ) => ()
      case Failure( NonFatal( exception ) ) =>
        connection foreach ( _.close() )
        throw exception
    }
  }
}
