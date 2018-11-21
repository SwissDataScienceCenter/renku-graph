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

import akka.stream.OverflowStrategy.backpressure
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.stream.{ Materializer, QueueOfferResult }
import akka.{ Done, NotUsed }
import ch.datascience.webhookservice.PushEvent
import javax.inject.{ Inject, Singleton }
import play.api.{ Logger, LoggerLike }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

@Singleton
class PushEventQueue(
    triplesFinder:   TriplesFinder,
    fusekiConnector: FusekiConnector,
    queueConfig:     QueueConfig,
    fileCommands:    Commands.File,
    logger:          LoggerLike
)( implicit executionContext: ExecutionContext, materializer: Materializer ) {

  @Inject() def this(
      triplesFinder:   TriplesFinder,
      fusekiConnector: FusekiConnector,
      queueConfig:     QueueConfig,
      fileCommands:    Commands.File
  )( implicit executionContext: ExecutionContext, materializer: Materializer ) =
    this( triplesFinder, fusekiConnector, queueConfig, fileCommands, Logger )

  import queueConfig._

  def offer( pushEvent: PushEvent ): Future[QueueOfferResult] =
    queue.offer( pushEvent )

  private lazy val queue = Source.queue[PushEvent](
    bufferSize.value,
    overflowStrategy = backpressure
  ).mapAsync( triplesFinderThreads.value )( pushEventToTriples )
    .flatMapConcat( logAndSkipErrors )
    .mapAsync( fusekiUploadThreads.value )( toFuseki )
    .map( deleteTriplesFile )
    .toMat( Sink.ignore )( Keep.left )
    .run()

  private def pushEventToTriples( pushEvent: PushEvent ): Future[( PushEvent, Either[Throwable, TriplesFile] )] =
    triplesFinder.generateTriples( pushEvent.gitRepositoryUrl, pushEvent.checkoutSha )
      .map( maybeTriplesFile => pushEvent -> maybeTriplesFile )

  private lazy val logAndSkipErrors: ( ( PushEvent, Either[Throwable, TriplesFile] ) ) => Source[( PushEvent, TriplesFile ), NotUsed] = {
    case ( event, Left( exception ) ) =>
      logger.error( s"Generating triples for $event failed: ${exception.getMessage}" )
      Source.empty[( PushEvent, TriplesFile )]
    case ( event, Right( triplesFile ) ) =>
      Source.single( event -> triplesFile )
  }

  private val toFuseki: ( ( PushEvent, TriplesFile ) ) => Future[TriplesFile] = {
    case ( event, triplesFile ) =>
      fusekiConnector
        .uploadFile( triplesFile )
        .map( _ => triplesFile )
        .recover {
          case NonFatal( exception: Exception ) =>
            logger.error( s"Uploading triples for $event failed: ${exception.getMessage}" )
            triplesFile
        }
  }

  private def deleteTriplesFile( triplesFile: TriplesFile ): Done = {
    fileCommands.removeSilently( triplesFile.value )
    Done
  }
}
