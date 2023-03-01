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
package tsprovisioning.minprojectinfo

import cats.{NonEmptyParallel, Parallel}
import cats.effect._
import cats.syntax.all._
import io.renku.events.{consumers, CategoryName}
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.ProcessExecutor
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: MonadCancelThrow: Logger](
    override val categoryName: CategoryName,
    tsReadinessChecker:        TSReadinessForEventsChecker[F],
    subscriptionMechanism:     SubscriptionMechanism[F],
    eventProcessor:            EventProcessor[F],
    processExecutor:           ProcessExecutor[F]
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  import io.renku.events.consumers.EventDecodingTools._

  protected override type Event = MinProjectInfoEvent

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      _.event.getProject.map(MinProjectInfoEvent(_)),
      eventProcessor.process,
      precondition = tsReadinessChecker.verifyTSReady,
      onRelease = subscriptionMechanism.renewSubscription().some
    )
}

private object EventHandler {
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
  ): F[consumers.EventHandler[F]] = for {
    tsReadinessChecker <- TSReadinessForEventsChecker[F]
    eventProcessor     <- EventProcessor[F]
    processesCount     <- find[F, Int Refined Positive]("add-min-project-info-max-concurrent-processes", config)
    processExecutor    <- ProcessExecutor.concurrent(processesCount)
  } yield new EventHandler[F](categoryName, tsReadinessChecker, subscriptionMechanism, eventProcessor, processExecutor)
}
