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
package events.categories.triplesgenerated

import cats.{MonadError, Show}
import cats.data.EitherT.{fromEither, fromOption}
import cats.effect.{ContextShift, Effect, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.events.consumers
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.events.consumers.subscriptions.SubscriptionMechanism
import ch.datascience.events.consumers.{EventRequestContent, EventSchedulingResult, Project}
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventBody}
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger
import io.circe.parser

import scala.concurrent.ExecutionContext

private[events] class EventHandler[Interpretation[_]: Effect](
    override val categoryName: CategoryName,
    eventsProcessingRunner:    EventsProcessingRunner[Interpretation],
    eventBodyDeserializer:     EventBodyDeserializer[Interpretation],
    logger:                    Logger[Interpretation]
)(implicit
    ME: MonadError[Interpretation, Throwable]
) extends consumers.EventHandler[Interpretation] {

  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import eventsProcessingRunner.scheduleForProcessing
  import io.circe.Decoder

  private type IdAndBody = (CompoundEventId, EventBody)

  override def handle(request: EventRequestContent): Interpretation[EventSchedulingResult] = {

    for {
      _       <- fromEither[Interpretation](request.event.validateCategoryName)
      eventId <- fromEither(request.event.getEventId)
      project <- fromEither(request.event.getProject)
      eventBodyJson <-
        fromOption[Interpretation](request.maybePayload.flatMap(str => parser.parse(str).toOption), BadRequest)
      eventBodyAndSchema <- fromEither(eventBodyJson.as[(EventBody, SchemaVersion)]).leftMap(_ => BadRequest)
      (eventBody, schemaVersion) = eventBodyAndSchema
      triplesGeneratedEvent <-
        eventBodyDeserializer
          .toTriplesGeneratedEvent(eventId, project, schemaVersion, eventBody)
          .toRightT(recoverTo = BadRequest)
      result <- scheduleForProcessing(triplesGeneratedEvent).toRightT
                  .semiflatTap(logger.log(eventId -> triplesGeneratedEvent.project))
                  .leftSemiflatTap(logger.log(eventId -> triplesGeneratedEvent.project))
    } yield result
  }.merge

  private implicit lazy val eventInfoToString: Show[(CompoundEventId, Project)] = Show.show { case (eventId, project) =>
    s"$eventId, projectPath = ${project.path}"
  }

  private implicit val eventBodyDecoder: Decoder[(EventBody, SchemaVersion)] = { implicit cursor =>
    for {
      schemaVersion <- cursor.downField("schemaVersion").as[SchemaVersion]
      eventBody     <- cursor.downField("payload").as[EventBody]
    } yield (eventBody, schemaVersion)
  }
}

private[events] object EventHandler {

  def apply(
      metricsRegistry:       MetricsRegistry[IO],
      gitLabThrottler:       Throttler[IO, GitLab],
      timeRecorder:          SparqlQueryTimeRecorder[IO],
      subscriptionMechanism: SubscriptionMechanism[IO],
      logger:                Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    processingRunner <-
      IOEventsProcessingRunner(metricsRegistry, gitLabThrottler, timeRecorder, subscriptionMechanism, logger)
  } yield new EventHandler[IO](categoryName, processingRunner, EventBodyDeserializer(), logger)
}
