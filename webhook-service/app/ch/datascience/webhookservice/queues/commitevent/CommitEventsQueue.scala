/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.queues.commitevent

import akka.stream.OverflowStrategy.backpressure
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.stream.{ IOResult, Materializer, QueueOfferResult }
import ch.datascience.graph.events.CommitEvent
import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }

@Singleton
class CommitEventsQueue(
    queueConfig:  QueueConfig,
    eventLogSink: Sink[CommitEvent, Future[IOResult]]
)( implicit executionContext: ExecutionContext, materializer: Materializer ) {

  @Inject() def this(
      queueConfig:          QueueConfig,
      eventLogSinkProvider: EventLogSinkProvider
  )( implicit executionContext: ExecutionContext, materializer: Materializer ) = this(
    queueConfig,
    eventLogSinkProvider.get
  )

  import queueConfig._

  def offer( commitEvent: CommitEvent ): Future[QueueOfferResult] =
    queue offer commitEvent

  private lazy val queue = Source
    .queue[CommitEvent]( bufferSize.value, overflowStrategy = backpressure )
    .toMat( eventLogSink )( Keep.left )
    .run()
}

trait EventLogSinkProvider {
  def get: Sink[CommitEvent, Future[IOResult]]
}
