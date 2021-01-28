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

import cats.MonadError
import cats.data.{EitherT, NonEmptyList}
import cats.effect.{ContextShift, Effect, IO, Timer}
import cats.implicits.catsSyntaxApplicativeId
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventBody, EventId}
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.EventSchedulingResult
import ch.datascience.triplesgenerator.events.EventSchedulingResult._
import ch.datascience.triplesgenerator.events.IOEventEndpoint.EventRequestContent
import ch.datascience.triplesgenerator.events.subscriptions.SubscriptionMechanismRegistry
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

private[events] class EventHandler[Interpretation[_]: Effect](
    eventsProcessingRunner: EventsProcessingRunner[Interpretation],
    eventBodyDeserializer:  EventBodyDeserializer[Interpretation],
    currentVersionPair:     RenkuVersionPair,
    logger:                 Logger[Interpretation]
)(implicit
    ME: MonadError[Interpretation, Throwable]
) extends events.EventHandler[Interpretation] {

  import ch.datascience.graph.model.projects
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import currentVersionPair.schemaVersion
  import eventBodyDeserializer.toCommitEvents
  import eventsProcessingRunner.scheduleForProcessing
  import io.circe.Decoder
  import org.http4s._
  import org.http4s.circe._

  override val categoryName: CategoryName = EventHandler.categoryName

  override def handle(requestContent: EventRequestContent): Interpretation[EventSchedulingResult] = {
    for {
      eventId <-
        EitherT(requestContent.event.as[CompoundEventId].pure[Interpretation]).leftMap(_ => UnsupportedEventType)
      eventBody <-
        EitherT.fromOption[Interpretation](requestContent.maybePayload.map(EventBody.apply), BadRequest)
      commitEvents <- toCommitEvents(eventBody).toRightT(recoverTo = BadRequest)
      result <- scheduleForProcessing(eventId, commitEvents, schemaVersion).toRightT
                  .semiflatTap(logger.log(eventId -> commitEvents))
                  .leftSemiflatTap(logger.log(eventId -> commitEvents))
    } yield result
  }.merge

  private implicit lazy val eventInfoToString: ((CompoundEventId, NonEmptyList[CommitEvent])) => String = {
    case (eventId, events) => s"$eventId, projectPath = ${events.head.project.path}"
  }

  private implicit lazy val payloadDecoder: EntityDecoder[Interpretation, CompoundEventId] =
    jsonOf[Interpretation, CompoundEventId]

  private implicit val eventDecoder: Decoder[CompoundEventId] = implicit cursor =>
    for {
      _         <- validateCategoryName
      id        <- cursor.downField("id").as[EventId]
      projectId <- cursor.downField("project").downField("id").as[projects.Id]
    } yield CompoundEventId(id, projectId)
}

private[events] object EventHandler {

  val categoryName: CategoryName = CategoryName("AWAITING_GENERATION")

  def apply(
      currentVersionPair:            RenkuVersionPair,
      metricsRegistry:               MetricsRegistry[IO],
      gitLabThrottler:               Throttler[IO, GitLab],
      timeRecorder:                  SparqlQueryTimeRecorder[IO],
      subscriptionMechanismRegistry: SubscriptionMechanismRegistry[IO],
      logger:                        Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    subscriptionMechanism <- subscriptionMechanismRegistry(categoryName)
    processingRunner <-
      IOEventsProcessingRunner(metricsRegistry, gitLabThrottler, timeRecorder, subscriptionMechanism, logger)
  } yield new EventHandler[IO](processingRunner, EventBodyDeserializer(), currentVersionPair, logger)
}
