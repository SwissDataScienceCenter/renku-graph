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

package ch.datascience.webhookservice.queues.logevent

import akka.Done
import akka.stream.Materializer
import akka.stream.OverflowStrategy.backpressure
import akka.stream.scaladsl.{ Keep, Sink, Source }
import javax.inject.{ Inject, Singleton }
import play.api.libs.json.JsValue
import play.api.{ Logger, LoggerLike }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

@Singleton
class LogEventQueue(
    queueConfig:     QueueConfig,
    logEventsSource: Source[Try[JsValue], Future[Done]],
    logger:          LoggerLike
)( implicit executionContext: ExecutionContext, materializer: Materializer ) {

  @Inject() def this(
      queueConfig:             QueueConfig,
      logEventsSourceProvider: EventLogSourceProvider
  )( implicit executionContext: ExecutionContext, materializer: Materializer ) = this(
    queueConfig,
    logEventsSourceProvider.get,
    Logger
  )

  import queueConfig._

  private val queue = Source.queue[JsValue](
    bufferSize.value,
    overflowStrategy = backpressure
  ).toMat( Sink.ignore )( Keep.left )
    .run()

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
}

trait EventLogSourceProvider {
  def get: Source[Try[JsValue], Future[Done]]
}
