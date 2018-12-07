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

import java.time.Instant

import akka.stream.OverflowStrategy.backpressure
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.stream.{ Materializer, QueueOfferResult }
import akka.{ Done, NotUsed }
import ch.datascience.graph.events.{ CommitEvent, User }
import ch.datascience.webhookservice.queues.commitevent.CommitEventsQueue
import javax.inject.{ Inject, Singleton }
import play.api.{ Logger, LoggerLike }

import scala.concurrent.{ ExecutionContext, Future }

@Singleton
class PushEventQueue(
    queueConfig:       QueueConfig,
    commitsEventQueue: CommitEventsQueue,
    logger:            LoggerLike
)( implicit executionContext: ExecutionContext, materializer: Materializer ) {

  @Inject() def this(
      queueConfig:       QueueConfig,
      commitsEventQueue: CommitEventsQueue
  )( implicit executionContext: ExecutionContext, materializer: Materializer ) =
    this( queueConfig, commitsEventQueue, Logger )

  import queueConfig._

  def offer( pushEvent: PushEvent ): Future[QueueOfferResult] =
    queue offer pushEvent

  private lazy val queue =
    Source
      .queue[PushEvent]( bufferSize.value, overflowStrategy = backpressure )
      .mapAsync( commitDetailsParallelism.value )( pushEventToCommitEvent )
      .flatMapConcat( logAndSkipCommitEventErrors )
      .toMat( commitEventsQueue )( Keep.left )
      .run()

  private def pushEventToCommitEvent( pushEvent: PushEvent ): Future[( PushEvent, Either[Throwable, CommitEvent] )] = Future {
    pushEvent -> Right(
      CommitEvent(
        pushEvent.after,
        "",
        Instant.EPOCH,
        pushEvent.pushUser,
        author    = User( pushEvent.pushUser.username, pushEvent.pushUser.email ),
        committer = User( pushEvent.pushUser.username, pushEvent.pushUser.email ),
        parents   = Seq( pushEvent.before ),
        project   = pushEvent.project,
        added     = Nil,
        modified  = Nil,
        removed   = Nil
      )
    )
  }

  private lazy val logAndSkipCommitEventErrors: ( ( PushEvent, Either[Throwable, CommitEvent] ) ) => Source[CommitEvent, NotUsed] = {
    case ( pushEvent, Left( exception ) ) =>
      logger.error( s"Generating CommitEvent for $pushEvent failed", exception )
      Source.empty[CommitEvent]
    case ( _, Right( commitEvent ) ) =>
      Source.single( commitEvent )
  }

  private lazy val commitEventsQueue: Sink[CommitEvent, Future[Done]] =
    Flow[CommitEvent]
      .mapAsync( commitDetailsParallelism.value ) { commitEvent =>
        commitsEventQueue
          .offer( commitEvent )
          .map( _ => logger.info( s"$commitEvent enqueued" ) )
          .recover {
            case exception =>
              logger.error( s"Enqueueing CommitEvent failed: $commitEvent", exception )
              Done
          }
      }
      .toMat( Sink.ignore )( Keep.right )
}
