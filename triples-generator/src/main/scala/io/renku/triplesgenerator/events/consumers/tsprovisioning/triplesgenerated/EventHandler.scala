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

package io.renku.triplesgenerator.events.consumers
package tsprovisioning.triplesgenerated

import cats.{NonEmptyParallel, Parallel}
import cats.effect._
import cats.syntax.all._
import io.renku.events.{CategoryName, consumers}
import io.renku.events.consumers.ProcessExecutor
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.metrics.MetricsRegistry
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger
import tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus

private class EventHandler[F[_]: MonadCancelThrow: Logger](
    override val categoryName: CategoryName,
    tsReadinessChecker:        TSReadinessForEventsChecker[F],
    eventDecoder:              EventDecoder,
    subscriptionMechanism:     SubscriptionMechanism[F],
    eventProcessor:            EventProcessor[F],
    processExecutor:           ProcessExecutor[F]
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  protected override type Event = TriplesGeneratedEvent

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      eventDecoder.decode,
      eventProcessor.process,
      precondition = tsReadinessChecker.verifyTSReady,
      onRelease = subscriptionMechanism.renewSubscription().some
    )
}

private object EventHandler {

  def apply[F[
      _
  ]: Async: NonEmptyParallel: Parallel: ReProvisioningStatus: GitLabClient: AccessTokenFinder: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      subscriptionMechanism:     SubscriptionMechanism[F],
      concurrentProcessesNumber: ConcurrentProcessesNumber
  ): F[consumers.EventHandler[F]] = for {
    tsReadinessChecker <- TSReadinessForEventsChecker[F]
    eventProcessor     <- EventProcessor[F]
    processExecutor    <- ProcessExecutor.concurrent(concurrentProcessesNumber.asRefined)
  } yield new EventHandler[F](categoryName,
                              tsReadinessChecker,
                              EventDecoder(),
                              subscriptionMechanism,
                              eventProcessor,
                              processExecutor
  )
}
