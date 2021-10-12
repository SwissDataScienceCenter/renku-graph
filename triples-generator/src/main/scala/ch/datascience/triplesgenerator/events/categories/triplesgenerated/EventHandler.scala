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

import cats.data.EitherT.{fromEither, fromOption}
import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, IO, Timer}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import ch.datascience.config.{ConfigLoader, GitLab}
import ch.datascience.control.Throttler
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.events.consumers.subscriptions.SubscriptionMechanism
import ch.datascience.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess, Project}
import ch.datascience.events.{EventRequestContent, consumers}
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventBody}
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.circe.parser.{parse => parseJson}
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
  import eventBodyDeserializer._
  import eventProcessor._
  import io.circe.Decoder

  private type IdAndBody = (CompoundEventId, EventBody)

  override def createHandlingProcess(
      request: EventRequestContent
  ): Interpretation[EventHandlingProcess[Interpretation]] =
    EventHandlingProcess.withWaitingForCompletion[Interpretation](
      processing => startProcessingEvent(request, processing),
      releaseProcess = subscriptionMechanism.renewSubscription()
    )

  private def startProcessingEvent(request: EventRequestContent, processing: Deferred[Interpretation, Unit]) = for {
    eventId            <- fromEither(request.event.getEventId)
    project            <- fromEither(request.event.getProject)
    eventBodyString    <- fromOption(request.maybePayload, BadRequest)
    eventBodyJson      <- fromEither(parseJson(eventBodyString)).leftMap(_ => BadRequest)
    eventBodyAndSchema <- fromEither(eventBodyJson.as[(EventBody, SchemaVersion)]).leftMap(_ => BadRequest)
    event              <- toEvent(eventId, project, eventBodyAndSchema).toRightT(recoverTo = BadRequest)
    result <- (ContextShift[Interpretation].shift *> Concurrent[Interpretation]
                .start(process(event) >> processing.complete(()))).toRightT
                .map(_ => Accepted)
                .semiflatTap(logger.log(eventId -> event.project))
                .leftSemiflatTap(logger.log(eventId -> event.project))
  } yield result

  private implicit lazy val eventInfoShow: Show[(CompoundEventId, Project)] = Show.show { case (eventId, project) =>
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
      config:                Config = ConfigFactory.load()
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO],
      logger:           Logger[IO]
  ): IO[EventHandler[IO]] = for {
    generationProcesses        <- find[IO, Int Refined Positive]("transformation-processes-number", config)
    eventProcessor             <- EventProcessor(metricsRegistry, gitLabThrottler, timeRecorder)
    concurrentProcessesLimiter <- ConcurrentProcessesLimiter(generationProcesses)
  } yield new EventHandler[IO](categoryName,
                               EventBodyDeserializer(),
                               subscriptionMechanism,
                               concurrentProcessesLimiter,
                               eventProcessor,
                               logger
  )
}
