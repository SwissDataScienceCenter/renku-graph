/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.awaitinggeneration

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.kernel.Deferred
import cats.effect.{Async, Concurrent}
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess}
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.graph.model.events.EventBody
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.events.consumers.TSReadinessForEventsChecker
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

private[events] class EventHandler[F[_]: MonadThrow: Concurrent: Logger](
    override val categoryName:  CategoryName,
    tsReadinessChecker:         TSReadinessForEventsChecker[F],
    eventProcessor:             EventProcessor[F],
    eventBodyDeserializer:      EventBodyDeserializer[F],
    subscriptionMechanism:      SubscriptionMechanism[F],
    concurrentProcessesLimiter: ConcurrentProcessesLimiter[F]
) extends consumers.EventHandlerWithProcessLimiter[F](concurrentProcessesLimiter) {

  import eventBodyDeserializer.toCommitEvent
  import eventProcessor._
  import tsReadinessChecker.verifyTSReady

  override def createHandlingProcess(requestContent: EventRequestContent): F[EventHandlingProcess[F]] =
    EventHandlingProcess.withWaitingForCompletion[F](
      verifyTSReady >> startProcessingEvent(requestContent, _),
      subscriptionMechanism.renewSubscription()
    )

  private def startProcessingEvent(requestContent: EventRequestContent, processing: Deferred[F, Unit]) = for {
    eventBody <- requestContent match {
                   case EventRequestContent.WithPayload(_, payload: String) => EitherT.rightT(EventBody(payload))
                   case _                                                   => EitherT.leftT(BadRequest)
                 }
    event <- toCommitEvent(eventBody).toRightT(recoverTo = BadRequest)
    result <- Concurrent[F]
                .start((process(event) recoverWith logError(event)) >> processing.complete(()))
                .toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(event))
                .leftSemiflatTap(Logger[F].log(event))
  } yield result
}

object EventHandler {

  def apply[F[_]: Async: ReProvisioningStatus: Logger: AccessTokenFinder: MetricsRegistry: SparqlQueryTimeRecorder](
      subscriptionMechanism: SubscriptionMechanism[F],
      config:                Config = ConfigFactory.load()
  ): F[EventHandler[F]] = for {
    tsReadinessChecker       <- TSReadinessForEventsChecker[F]
    eventProcessor           <- EventProcessor[F]
    generationProcesses      <- GenerationProcessesNumber[F](config)
    concurrentProcessLimiter <- ConcurrentProcessesLimiter(Refined.unsafeApply(generationProcesses.value))
  } yield new EventHandler[F](categoryName,
                              tsReadinessChecker,
                              eventProcessor,
                              EventBodyDeserializer[F],
                              subscriptionMechanism,
                              concurrentProcessLimiter
  )
}
