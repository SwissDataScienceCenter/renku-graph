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
  import OneParentCommit._
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
    .map( toOneParentCommits )
    .flatMapConcat( logAndSkipJsonErrors )
    .mapAsync( triplesFinderThreads.value )( oneParentCommitToRdfTriples )
    .flatMapConcat( logAndSkipTriplesErrors )
    .toMat( fusekiSink )( Keep.left )
    .run()

  private def toOneParentCommits( jsValue: JsValue ): Either[Throwable, List[OneParentCommit]] =
    jsValue.validate[List[OneParentCommit]] match {
      case JsSuccess( oneParentCommit, _ ) => Right( oneParentCommit )
      case JsError( errors ) => Left(
        new RuntimeException(
          errors.foldLeft( "Json deserialization error(s):" ) {
            case ( message, ( path, pathErrors ) ) =>
              s"$message\n $path -> ${pathErrors.map( _.message ).mkString( "; " )}"
          }
        )
      )
    }

  private lazy val logAndSkipJsonErrors: Either[Throwable, List[OneParentCommit]] => Source[OneParentCommit, NotUsed] = {
    case Left( exception ) =>
      logger.error( s"Invalid event data received from Event Log: ${exception.getMessage}" )
      Source.empty[OneParentCommit]
    case Right( oneParentCommits ) =>
      Source[OneParentCommit]( oneParentCommits )
  }

  private def oneParentCommitToRdfTriples( oneParentCommit: OneParentCommit ): Future[( OneParentCommit, Either[Throwable, RDFTriples] )] =
    triplesFinder.generateTriples( oneParentCommit.projectPath, oneParentCommit.id )
      .map( maybeTriplesFile => oneParentCommit -> maybeTriplesFile )

  private lazy val logAndSkipTriplesErrors: ( ( OneParentCommit, Either[Throwable, RDFTriples] ) ) => Source[( OneParentCommit, RDFTriples ), NotUsed] = {
    case ( oneParentCommit, Left( exception ) ) =>
      logger.error( s"Generating triples for $oneParentCommit failed", exception )
      Source.empty[( OneParentCommit, RDFTriples )]
    case ( oneParentCommit, Right( triples ) ) =>
      Source.single( oneParentCommit -> triples )
  }

  private lazy val fusekiSink: Sink[( OneParentCommit, RDFTriples ), Future[Done]] =
    Flow[( OneParentCommit, RDFTriples )]
      .mapAsync( fusekiUploadThreads.value ) {
        case ( oneParentCommit, triplesFile ) =>
          fusekiConnector
            .upload( triplesFile )
            .map( _ => logger.info( s"Triples for $oneParentCommit uploaded to triples store" ) )
            .recover {
              case exception =>
                logger.error( s"Uploading triples for $oneParentCommit failed", exception )
                Done
            }
      }
      .toMat( Sink.ignore )( Keep.right )
}

private object LogEventQueue {

  private[logevent] case class OneParentCommit(
      id:            CommitId,
      projectPath:   ProjectPath,
      maybeParentId: Option[CommitId]
  )

  private[logevent] object OneParentCommit {

    import play.api.libs.functional.syntax._
    import play.api.libs.json.Reads._
    import play.api.libs.json._

    implicit val oneParentCommitsReads: Reads[List[OneParentCommit]] = (
      ( __ \ "id" ).read[CommitId] and
      ( __ \ "project" \ "path" ).read[ProjectPath] and
      ( __ \ "parents" ).read[Seq[CommitId]]
    )( oneParentCommits _ )

    private def oneParentCommits( commitId: CommitId, projectPath: ProjectPath, parents: Seq[CommitId] ) =
      parents match {
        case Nil =>
          List( OneParentCommit( commitId, projectPath, maybeParentId = None ) )
        case someParents =>
          someParents
            .map( parent => OneParentCommit( commitId, projectPath, maybeParentId = Some( parent ) ) )
            .toList
      }
  }
}

trait EventLogSourceProvider {
  def get: Source[Try[JsValue], Future[Done]]
}
