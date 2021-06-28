/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator
package events.categories.awaitinggeneration

import cats.data.EitherT.{fromEither, fromOption}
import cats.effect.{ContextShift, Effect, IO, Timer}
import cats.{MonadThrow, Show}
import ch.datascience.events.consumers.EventSchedulingResult
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.events.consumers.subscriptions.SubscriptionMechanism
import ch.datascience.events.{EventRequestContent, consumers}
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.graph.model.events.{CategoryName, EventBody}
import ch.datascience.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[events] class EventHandler[Interpretation[_]: Effect: MonadThrow](
    override val categoryName: CategoryName,
    eventsProcessingRunner:    EventsProcessingRunner[Interpretation],
    eventBodyDeserializer:     EventBodyDeserializer[Interpretation],
    currentVersionPair:        RenkuVersionPair,
    logger:                    Logger[Interpretation]
) extends consumers.EventHandler[Interpretation] {

  import currentVersionPair.schemaVersion
  import eventBodyDeserializer.toCommitEvent
  import eventsProcessingRunner.scheduleForProcessing

  override def handle(requestContent: EventRequestContent): Interpretation[EventSchedulingResult] = {
    for {
      _           <- fromEither[Interpretation](requestContent.event.validateCategoryName)
      eventBody   <- fromOption[Interpretation](requestContent.maybePayload.map(EventBody.apply), BadRequest)
      commitEvent <- toCommitEvent(eventBody).toRightT(recoverTo = BadRequest)
      result <- scheduleForProcessing(commitEvent, schemaVersion).toRightT
                  .semiflatTap(logger.log(commitEvent))
                  .leftSemiflatTap(logger.log(commitEvent))
    } yield result
  }.merge

  private implicit lazy val eventInfoToString: Show[CommitEvent] = Show.show { event =>
    s"${event.compoundEventId}, projectPath = ${event.project.path}"
  }
}

object EventHandler {

  def apply(
      currentVersionPair:    RenkuVersionPair,
      metricsRegistry:       MetricsRegistry[IO],
      subscriptionMechanism: SubscriptionMechanism[IO],
      logger:                Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    processingRunner <-
      IOEventsProcessingRunner(metricsRegistry, subscriptionMechanism, logger)
  } yield new EventHandler[IO](categoryName, processingRunner, EventBodyDeserializer(), currentVersionPair, logger)
}
