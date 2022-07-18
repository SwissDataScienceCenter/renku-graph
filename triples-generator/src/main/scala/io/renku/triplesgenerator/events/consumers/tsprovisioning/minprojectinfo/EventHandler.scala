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
package tsprovisioning.minprojectinfo

import cats.data.EitherT.fromEither
import cats.effect._
import cats.syntax.all._
import cats.{NonEmptyParallel, Parallel}
import io.renku.events.consumers.EventSchedulingResult.Accepted
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess}
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.metrics.MetricsRegistry
import io.renku.triplesstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import org.typelevel.log4cats.Logger

private[events] class EventHandler[F[_]: Concurrent: Logger](
    override val categoryName:  CategoryName,
    concurrentProcessesLimiter: ConcurrentProcessesLimiter[F],
    tsReadinessChecker:         TSReadinessForEventsChecker[F],
    subscriptionMechanism:      SubscriptionMechanism[F],
    eventProcessor:             EventProcessor[F]
) extends consumers.EventHandlerWithProcessLimiter[F](concurrentProcessesLimiter) {

  import eventProcessor._
  import tsReadinessChecker._

  override def createHandlingProcess(request: EventRequestContent) =
    EventHandlingProcess.withWaitingForCompletion[F](
      verifyTSReady >> startProcessingEvent(request, _),
      releaseProcess = subscriptionMechanism.renewSubscription()
    )

  private def startProcessingEvent(request: EventRequestContent, processing: Deferred[F, Unit]) = for {
    project <- fromEither(request.event.getProject)
    result <- Spawn[F]
                .start(process(MinProjectInfoEvent(project)) >> processing.complete(()))
                .toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(project))
                .leftSemiflatTap(Logger[F].log(project))
  } yield result
}

private[events] object EventHandler {
  import com.typesafe.config.{Config, ConfigFactory}
  import eu.timepit.refined.api.Refined
  import eu.timepit.refined.numeric.Positive
  import eu.timepit.refined.pureconfig._
  import io.renku.config.ConfigLoader.find

  def apply[F[
      _
  ]: Async: NonEmptyParallel: Parallel: ReProvisioningStatus: GitLabClient: AccessTokenFinder: MetricsRegistry: Logger: SparqlQueryTimeRecorder](
      subscriptionMechanism: SubscriptionMechanism[F],
      config:                Config = ConfigFactory.load()
  ): F[EventHandler[F]] = for {
    maxConcurrentProcesses     <- find[F, Int Refined Positive]("add-min-project-info-max-concurrent-processes", config)
    concurrentProcessesLimiter <- ConcurrentProcessesLimiter(maxConcurrentProcesses)
    tsReadinessChecker         <- TSReadinessForEventsChecker[F]
    eventProcessor             <- EventProcessor[F]
  } yield new EventHandler[F](categoryName,
                              concurrentProcessesLimiter,
                              tsReadinessChecker,
                              subscriptionMechanism,
                              eventProcessor
  )
}
