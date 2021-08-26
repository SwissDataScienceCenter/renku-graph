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

import cats.MonadThrow
import cats.data.EitherT.{fromEither, fromOption}
import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.{ConfigLoader, GitLab}
import ch.datascience.control.Throttler
import ch.datascience.events.consumers
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.events.consumers.subscriptions.SubscriptionMechanism
import ch.datascience.events.consumers.{ConcurrentProcessesLimiter, EventRequestContent, EventSchedulingResult, Project}
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventBody}
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.circe.parser
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[events] class EventHandler[Interpretation[_]: ConcurrentEffect: MonadThrow: ContextShift](
    override val categoryName:  CategoryName,
    eventBodyDeserializer:      EventBodyDeserializer[Interpretation],
    subscriptionMechanism:      SubscriptionMechanism[Interpretation],
    concurrentProcessesLimiter: ConcurrentProcessesLimiter[Interpretation],
    eventProcessor:             EventProcessor[Interpretation],
    logger:                     Logger[Interpretation]
) extends consumers.EventHandlerWithProcessLimiter[Interpretation](concurrentProcessesLimiter) {

  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe.Decoder

  private type IdAndBody = (CompoundEventId, EventBody)

  override def maybeReleaseProcess: Option[Interpretation[Unit]] =
    Concurrent[Interpretation].start(subscriptionMechanism.renewSubscription()).void.some

  override def handle(
      request: EventRequestContent
  ): Interpretation[(Deferred[Interpretation, Unit], Interpretation[EventSchedulingResult])] =
    Deferred[Interpretation, Unit].map(done => done -> startProcessingEvent(request, done))

  private def startProcessingEvent(request: EventRequestContent, done: Deferred[Interpretation, Unit]) = {
    for {
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
      result <- (ContextShift[Interpretation].shift *> Concurrent[Interpretation]
                  .start(eventProcessor.process(triplesGeneratedEvent).flatMap(_ => done.complete(())))).toRightT
                  .map(_ => Accepted)
                  .semiflatTap(logger.log(eventId -> triplesGeneratedEvent.project))
                  .leftSemiflatTap(logger.log(eventId -> triplesGeneratedEvent.project))

    } yield result
  }.merge

  private implicit lazy val eventInfoToString: ((CompoundEventId, Project)) => String = { case (eventId, project) =>
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
  import ConfigLoader.find
  import eu.timepit.refined.pureconfig._

  def apply(
      metricsRegistry:       MetricsRegistry[IO],
      gitLabThrottler:       Throttler[IO, GitLab],
      timeRecorder:          SparqlQueryTimeRecorder[IO],
      subscriptionMechanism: SubscriptionMechanism[IO],
      logger:                Logger[IO],
      config:                Config = ConfigFactory.load()
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    generationProcesses <- find[IO, Int Refined Positive]("transformation-processes-number", config)
    eventProcessor      <- IOTriplesGeneratedEventProcessor(metricsRegistry, gitLabThrottler, timeRecorder, logger)
    concurrentProcessesLimiter <-
      ConcurrentProcessesLimiter(generationProcesses)
  } yield new EventHandler[IO](categoryName,
                               EventBodyDeserializer(),
                               subscriptionMechanism,
                               concurrentProcessesLimiter,
                               eventProcessor,
                               logger
  )
}
