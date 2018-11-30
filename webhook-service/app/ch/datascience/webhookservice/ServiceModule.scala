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

package ch.datascience.webhookservice

import java.net.URL
import java.nio.charset.Charset
import java.nio.file.Path

import akka.Done
import akka.stream.Materializer
import ch.datascience.webhookservice.config.{ FusekiConfig, ServiceUrl }
import ch.datascience.webhookservice.queues.commitevent.{ EventLogSinkProvider, FileEventLogSinkProvider }
import ch.datascience.webhookservice.queues.logevent.{ EventLogSourceProvider, FileEventLogSourceProvider, LogEventQueue }
import com.google.inject.AbstractModule
import com.google.inject.name.Names
import javax.inject.{ Inject, Singleton }
import play.api.libs.ws.{ WSAuthScheme, WSClient, WSResponse }
import play.api.test.Helpers.CONTENT_TYPE
import play.api.{ Configuration, Environment, Logger, LoggerLike }

import scala.concurrent.{ ExecutionContext, Future }

class ServiceModule(
    environment:   Environment,
    configuration: Configuration
) extends AbstractModule {

  override def configure(): Unit = {

    bind( classOf[FusekiDatasetVerifier] )
      .asEagerSingleton()

    bind( classOf[URL] )
      .annotatedWith( Names.named( "gitlabUrl" ) )
      .toInstance( configuration.get[ServiceUrl]( "services.gitlab.url" ).value )

    bind( classOf[Path] )
      .annotatedWith( Names.named( "event-log-file-path" ) )
      .toInstance {
        import java.nio.file._
        val path = FileSystems.getDefault.getPath( "/tmp/renku-event.log" )
        val file = Files.createFile( path )
        file.toFile.deleteOnExit()
        path
      }

    bind( classOf[EventLogSinkProvider] )
      .to( classOf[FileEventLogSinkProvider] )

    bind( classOf[EventLogSourceProvider] )
      .to( classOf[FileEventLogSourceProvider] )

    bind( classOf[LogEventQueue] )
      .asEagerSingleton()
  }
}

@Singleton
private class FusekiDatasetVerifier(
    fusekiConfig: FusekiConfig,
    httpClient:   WSClient,
    logger:       LoggerLike
)( implicit executionContext: ExecutionContext, materializer: Materializer ) {

  @Inject() def this(
      fusekiConfig: FusekiConfig,
      httpClient:   WSClient
  )( implicit executionContext: ExecutionContext, materializer: Materializer ) =
    this( fusekiConfig, httpClient, Logger )

  import fusekiConfig._
  import play.api.http.Status._

  val assureDatasetExists: Future[Done] = {
    checkDatasetExists()
      .flatMap {
        case true  => Future.successful( Done )
        case false => createDataset()
      }
  }

  private def checkDatasetExists(): Future[Boolean] =
    httpClient
      .url( fusekiBaseUrl / "$" / "datasets" / datasetName )
      .withAuth( username.toString, password.toString, WSAuthScheme.BASIC )
      .get()
      .map( response => response.status -> response )
      .map {
        case ( OK, _ ) =>
          logger.info( s"'$datasetName' dataset exists in Jena; No action needed." )
          true
        case ( NOT_FOUND, _ ) =>
          logger.info( s"'$datasetName' dataset does not exist in Jena." )
          false
        case ( other, response ) =>
          val message = s"'$datasetName' dataset existence check failed with $other and message: ${response.bodyAsString}"
          logger.error( message )
          throw new RuntimeException( message )
      }

  private def createDataset(): Future[Done] =
    httpClient
      .url( fusekiBaseUrl / "$" / "datasets" )
      .withHttpHeaders( CONTENT_TYPE -> "application/x-www-form-urlencoded" )
      .withAuth( username.toString, password.toString, WSAuthScheme.BASIC )
      .post( Map( "dbName" -> datasetName.toString, "dbType" -> datasetType.toString ) )
      .map( response => response.status -> response )
      .map {
        case ( OK, _ ) =>
          logger.info( s"'$datasetName' created in Jena." )
          Done
        case ( other, response ) =>
          val message = s"'$datasetName' dataset creation failed with $other and message: ${response.bodyAsString}"
          logger.error( message )
          throw new RuntimeException( message )
      }

  private implicit class ResponseOps( response: WSResponse ) {

    lazy val bodyAsString: String =
      response
        .bodyAsBytes
        .decodeString( Charset.forName( "utf-8" ) )
  }
}
