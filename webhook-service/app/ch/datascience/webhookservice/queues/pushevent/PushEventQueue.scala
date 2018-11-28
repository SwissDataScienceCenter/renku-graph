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

package ch.datascience.webhookservice.queues.pushevent

import akka.stream.OverflowStrategy.backpressure
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.stream.{ Materializer, QueueOfferResult }
import akka.{ Done, NotUsed }
import javax.inject.{ Inject, Singleton }
import play.api.{ Logger, LoggerLike }

import scala.concurrent.{ ExecutionContext, Future }

@Singleton
class PushEventQueue(
    triplesFinder:   TriplesFinder,
    fusekiConnector: FusekiConnector,
    queueConfig:     QueueConfig,
    logger:          LoggerLike
)( implicit executionContext: ExecutionContext, materializer: Materializer ) {

  @Inject() def this(
      triplesFinder:   TriplesFinder,
      fusekiConnector: FusekiConnector,
      queueConfig:     QueueConfig
  )( implicit executionContext: ExecutionContext, materializer: Materializer ) =
    this( triplesFinder, fusekiConnector, queueConfig, Logger )

  import queueConfig._

  def offer( pushEvent: PushEvent ): Future[QueueOfferResult] =
    queue offer pushEvent

  private lazy val queue = Source.queue[PushEvent](
    bufferSize.value,
    overflowStrategy = backpressure
  ).mapAsync( triplesFinderThreads.value )( pushEventToRdfTriples )
    .flatMapConcat( logAndSkipErrors )
    .toMat( fusekiSink )( Keep.left )
    .run()

  private def pushEventToRdfTriples( pushEvent: PushEvent ): Future[( PushEvent, Either[Throwable, RDFTriples] )] =
    triplesFinder.generateTriples( pushEvent.project.path, pushEvent.before )
      .map( maybeTriplesFile => pushEvent -> maybeTriplesFile )

  private lazy val logAndSkipErrors: ( ( PushEvent, Either[Throwable, RDFTriples] ) ) => Source[( PushEvent, RDFTriples ), NotUsed] = {
    case ( event, Left( exception ) ) =>
      logger.error( s"Generating triples for $event failed: ${exception.getMessage}" )
      Source.empty[( PushEvent, RDFTriples )]
    case ( event, Right( triplesFile ) ) =>
      Source.single( event -> triplesFile )
  }

  private lazy val fusekiSink: Sink[( PushEvent, RDFTriples ), Future[Done]] =
    Flow[( PushEvent, RDFTriples )]
      .mapAsync( fusekiUploadThreads.value ) {
        case ( event, triplesFile ) =>
          fusekiConnector
            .uploadFile( triplesFile )
            .recover {
              case exception =>
                logger.error( s"Uploading triples for $event failed: ${exception.getMessage}" )
                Done
            }
      }
      .toMat( Sink.ignore )( Keep.right )
}
