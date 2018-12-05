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

package ch.datascience.triplesgenerator.queues.logevent

import akka.stream.Materializer
import akka.stream.OverflowStrategy.backpressure
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.{ Done, NotUsed }
import ch.datascience.graph.events.{ CommitId, ProjectPath }
import javax.inject.{ Inject, Singleton }
import play.api.libs.json.{ JsError, JsSuccess, JsValue }
import play.api.{ Logger, LoggerLike }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

@Singleton
class LogEventQueue(
    queueConfig:     QueueConfig,
    logEventsSource: Source[Try[JsValue], Future[Done]],
    triplesFinder:   TriplesFinder,
    fusekiConnector: FusekiConnector,
    logger:          LoggerLike
)( implicit executionContext: ExecutionContext, materializer: Materializer ) {

  @Inject() def this(
      queueConfig:             QueueConfig,
      logEventsSourceProvider: EventLogSourceProvider,
      triplesFinder:           TriplesFinder,
      fusekiConnector:         FusekiConnector
  )( implicit executionContext: ExecutionContext, materializer: Materializer ) = this(
    queueConfig,
    logEventsSourceProvider.get,
    triplesFinder,
    fusekiConnector,
    Logger
  )

  import LogEventQueue._
  import queueConfig._

  logEventsSource
    .mapAsync( bufferSize.value ) {
      case Failure( throwable ) =>
        logger.error( "Received broken notification from Event Log", throwable )
        Future.successful( Done )
      case Success( eventAsJson ) =>
        logger.info( s"Received event from Event Log: $eventAsJson" )
        queue.offer( eventAsJson )
    }.toMat( Sink.ignore )( Keep.none )
    .run()

  private lazy val queue = Source
    .queue[JsValue]( bufferSize.value, overflowStrategy = backpressure )
    .map( toCommitInfo )
    .flatMapConcat( logAndSkipJsonErrors )
    .mapAsync( triplesFinderThreads.value )( pushEventToRdfTriples )
    .flatMapConcat( logAndSkipTriplesErrors )
    .toMat( fusekiSink )( Keep.left )
    .run()

  private def toCommitInfo( jsValue: JsValue ): Either[Throwable, CommitInfo] = jsValue.validate[CommitInfo] match {
    case JsSuccess( commitInfo, _ ) => Right( commitInfo )
    case JsError( errors ) => Left(
      new RuntimeException(
        errors.foldLeft( "Json deserialization error(s):" ) {
          case ( message, ( path, pathErrors ) ) =>
            s"$message\n $path -> ${pathErrors.map( _.message ).mkString( "; " )}"
        }
      )
    )
  }

  private lazy val logAndSkipJsonErrors: Either[Throwable, CommitInfo] => Source[CommitInfo, NotUsed] = {
    case Left( exception ) =>
      logger.error( s"Invalid event data received from Event Log: ${exception.getMessage}" )
      Source.empty[CommitInfo]
    case Right( commitInfo ) =>
      Source.single( commitInfo )
  }

  private def pushEventToRdfTriples( commitInfo: CommitInfo ): Future[( CommitInfo, Either[Throwable, RDFTriples] )] =
    triplesFinder.generateTriples( commitInfo.projectPath, commitInfo.id )
      .map( maybeTriplesFile => commitInfo -> maybeTriplesFile )

  private lazy val logAndSkipTriplesErrors: ( ( CommitInfo, Either[Throwable, RDFTriples] ) ) => Source[( CommitInfo, RDFTriples ), NotUsed] = {
    case ( commitInfo, Left( exception ) ) =>
      logger.error( s"Generating triples for $commitInfo failed", exception )
      Source.empty[( CommitInfo, RDFTriples )]
    case ( commitInfo, Right( triples ) ) =>
      Source.single( commitInfo -> triples )
  }

  private lazy val fusekiSink: Sink[( CommitInfo, RDFTriples ), Future[Done]] =
    Flow[( CommitInfo, RDFTriples )]
      .mapAsync( fusekiUploadThreads.value ) {
        case ( commitInfo, triplesFile ) =>
          fusekiConnector
            .upload( triplesFile )
            .map( _ => logger.info( s"Triples for $commitInfo uploaded to triples store" ) )
            .recover {
              case exception =>
                logger.error( s"Uploading triples for $commitInfo failed", exception )
                Done
            }
      }
      .toMat( Sink.ignore )( Keep.right )
}

private object LogEventQueue {

  private[logevent] case class CommitInfo(
      id:          CommitId,
      projectPath: ProjectPath
  )

  private[logevent] object CommitInfo {

    import play.api.libs.functional.syntax._
    import play.api.libs.json.Reads._
    import play.api.libs.json._

    implicit val commitInfoReads: Reads[CommitInfo] = (
      ( __ \ "id" ).read[CommitId] and
      ( __ \ "project" \ "path" ).read[ProjectPath]
    )( CommitInfo.apply _ )
  }
}

trait EventLogSourceProvider {
  def get: Source[Try[JsValue], Future[Done]]
}
