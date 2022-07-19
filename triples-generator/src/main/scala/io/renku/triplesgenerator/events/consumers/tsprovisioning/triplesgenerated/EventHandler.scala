/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers
package tsprovisioning.triplesgenerated

import cats.data.EitherT
import cats.data.EitherT.fromEither
import cats.effect._
import cats.syntax.all._
import cats.{NonEmptyParallel, Parallel, Show}
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.renku.config.ConfigLoader
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess, Project}
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.graph.model.SchemaVersion
import io.renku.graph.model.events.{CompoundEventId, EventBody, ZippedEventPayload}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.metrics.MetricsRegistry
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger
import tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus

private[events] class EventHandler[F[_]: Concurrent: Logger](
    override val categoryName:  CategoryName,
    tsReadinessChecker:         TSReadinessForEventsChecker[F],
    eventBodyDeserializer:      EventBodyDeserializer[F],
    subscriptionMechanism:      SubscriptionMechanism[F],
    concurrentProcessesLimiter: ConcurrentProcessesLimiter[F],
    eventProcessor:             EventProcessor[F]
) extends consumers.EventHandlerWithProcessLimiter[F](concurrentProcessesLimiter) {

  import eventBodyDeserializer._
  import eventProcessor._
  import io.circe.Decoder
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import tsReadinessChecker._

  override def createHandlingProcess(request: EventRequestContent): F[EventHandlingProcess[F]] =
    EventHandlingProcess.withWaitingForCompletion[F](
      verifyTSReady >> startProcessingEvent(request, _),
      releaseProcess = subscriptionMechanism.renewSubscription()
    )

  private def startProcessingEvent(request: EventRequestContent, processing: Deferred[F, Unit]) = for {
    eventId <- fromEither(request.event.getEventId)
    project <- fromEither(request.event.getProject)
    payload <- request match {
                 case EventRequestContent.WithPayload(_, payload: ZippedEventPayload) => EitherT.rightT(payload)
                 case _                                                               => EitherT.leftT(BadRequest)
               }
    event <- toEvent(eventId, project, payload).toRightT(recoverTo = BadRequest)
    result <- Spawn[F]
                .start(process(event).recoverWith(errorLogging(event)) >> processing.complete(()))
                .toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(eventId -> event.project))
                .leftSemiflatTap(Logger[F].log(eventId -> event.project))
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

private object EventHandler {
  import ConfigLoader.find
  import eu.timepit.refined.pureconfig._

  def apply[F[
      _
  ]: Async: NonEmptyParallel: Parallel: ReProvisioningStatus: GitLabClient: AccessTokenFinder: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      subscriptionMechanism: SubscriptionMechanism[F],
      config:                Config = ConfigFactory.load()
  ): F[EventHandler[F]] = for {
    tsReadinessChecker         <- TSReadinessForEventsChecker[F]
    maxConcurrentProcesses     <- find[F, Int Refined Positive]("transformation-processes-number", config)
    eventProcessor             <- EventProcessor[F]
    concurrentProcessesLimiter <- ConcurrentProcessesLimiter(maxConcurrentProcesses)
  } yield new EventHandler[F](categoryName,
                              tsReadinessChecker,
                              EventBodyDeserializer[F],
                              subscriptionMechanism,
                              concurrentProcessesLimiter,
                              eventProcessor
  )
}
