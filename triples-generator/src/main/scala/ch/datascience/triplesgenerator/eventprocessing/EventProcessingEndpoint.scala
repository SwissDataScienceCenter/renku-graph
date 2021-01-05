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

package ch.datascience.triplesgenerator.eventprocessing

import cats.MonadError
import cats.data.NonEmptyList
import cats.effect.{Effect, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.events.{CompoundEventId, EventBody, EventId}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.config.TriplesGeneration
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator
import ch.datascience.triplesgenerator.models.RenkuVersionPair
import ch.datascience.triplesgenerator.reprovisioning.{ReProvisioningStatus, RenkuVersionPairUpdater}
import ch.datascience.triplesgenerator.subscriptions.Subscriber
import io.chrisdavenport.log4cats.Logger
import org.http4s.dsl.Http4sDsl

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

class EventProcessingEndpoint[Interpretation[_]: Effect](
    eventBodyDeserializer:  EventBodyDeserialiser[Interpretation],
    eventsProcessingRunner: EventsProcessingRunner[Interpretation],
    reProvisioningStatus:   ReProvisioningStatus[Interpretation],
    currentVersionPair:     RenkuVersionPair,
    logger:                 Logger[Interpretation]
)(implicit ME:              MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import EventProcessingEndpoint._
  import EventsProcessingRunner.EventSchedulingResult
  import cats.syntax.all._
  import ch.datascience.controllers.InfoMessage._
  import ch.datascience.controllers.{ErrorMessage, InfoMessage}
  import eventBodyDeserializer._
  import eventsProcessingRunner._
  import org.http4s._
  import org.http4s.circe._

  def processEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]] =
    reProvisioningStatus.isReProvisioning() flatMap { isReProvisioning =>
      if (isReProvisioning) ServiceUnavailable(InfoMessage("Temporarily unavailable: currently re-provisioning"))
      else process(request)
    }

  private def process(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      eventAndBody <- request.as[IdAndBody] recoverWith badRequest("Event deserialization error")
      commitEvents <- toCommitEvents(eventAndBody._2) recoverWith badRequest("Event body deserialization error")
      result       <- scheduleForProcessing(eventAndBody._1, commitEvents, currentVersionPair.schemaVersion)
      response     <- result.asHttpResponse(eventAndBody._1, commitEvents)
    } yield response
  } recoverWith httpResponse

  private def badRequest[O](message: String): PartialFunction[Throwable, Interpretation[O]] = {
    case NonFatal(exception) => ME.raiseError(BadRequestError(message, exception))
  }

  private implicit class ResultOps(result: EventSchedulingResult) {

    def asHttpResponse(eventId:      CompoundEventId,
                       commitEvents: NonEmptyList[CommitEvent]
    ): Interpretation[Response[Interpretation]] =
      result match {
        case EventSchedulingResult.Accepted =>
          logInfo(eventId, commitEvents.head.project.path)
          Accepted(InfoMessage("Event accepted for processing"))
        case EventSchedulingResult.Busy =>
          TooManyRequests(InfoMessage("Too many events under processing"))
      }

    private def logInfo(eventId: CompoundEventId, projectPath: projects.Path) = logger.info(
      s"Event $eventId, projectPath = $projectPath -> $result"
    )
  }

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case exception: BadRequestError => BadRequest(ErrorMessage(exception))
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage("Scheduling Event for processing failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  private case class BadRequestError(message: String, cause: Throwable) extends Exception(message, cause)

  private implicit lazy val payloadDecoder: EntityDecoder[Interpretation, IdAndBody] = jsonOf[Interpretation, IdAndBody]
}

object EventProcessingEndpoint {

  import ch.datascience.graph.model.projects
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe.{Decoder, HCursor}

  type IdAndBody = (CompoundEventId, EventBody)

  implicit val eventDecoder: Decoder[IdAndBody] = (cursor: HCursor) =>
    for {
      id        <- cursor.downField("id").as[EventId]
      projectId <- cursor.downField("project").downField("id").as[projects.Id]
      body      <- cursor.downField("body").as[EventBody]
    } yield CompoundEventId(id, projectId) -> body
}

object IOEventProcessingEndpoint {
  import cats.effect.{ContextShift, IO}

  def apply(
      subscriber:           Subscriber[IO],
      triplesGeneration:    TriplesGeneration,
      reProvisioningStatus: ReProvisioningStatus[IO],
      currentVersionPair:   RenkuVersionPair,
      metricsRegistry:      MetricsRegistry[IO],
      gitLabThrottler:      Throttler[IO, GitLab],
      timeRecorder:         SparqlQueryTimeRecorder[IO],
      logger:               Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[EventProcessingEndpoint[IO]] =
    for {
      triplesGenerator <- TriplesGenerator(triplesGeneration)
      commitEventProcessor <-
        IOCommitEventProcessor(triplesGenerator, metricsRegistry, gitLabThrottler, timeRecorder, logger)
      eventsProcessingRunner <- IOEventsProcessingRunner(commitEventProcessor, subscriber, logger)
      bodyDeserialiser = new EventBodyDeserialiser[IO]()
    } yield new EventProcessingEndpoint[IO](bodyDeserialiser,
                                            eventsProcessingRunner,
                                            reProvisioningStatus,
                                            currentVersionPair,
                                            logger
    )
}
