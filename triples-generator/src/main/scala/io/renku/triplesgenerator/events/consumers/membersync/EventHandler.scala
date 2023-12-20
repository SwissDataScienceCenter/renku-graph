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
package membersync

import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import com.typesafe.config.Config
import fs2.io.net.Network
import io.renku.events.consumers.ProcessExecutor
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.{CategoryName, consumers}
import io.renku.graph.model.projects
import io.renku.http.client.GitLabClient
import io.renku.lock.Lock
import io.renku.lock.syntax._
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.TgDB.TsWriteLock
import io.renku.triplesstore.{ProjectSparqlClient, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger
import tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus

private class EventHandler[F[_]: MonadCancelThrow: Logger](
    override val categoryName: CategoryName,
    tsReadinessChecker:        TSReadinessForEventsChecker[F],
    membersSynchronizer:       MembersSynchronizer[F],
    subscriptionMechanism:     SubscriptionMechanism[F],
    processExecutor:           ProcessExecutor[F],
    tsWriteLock:               TsWriteLock[F]
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  import io.renku.events.consumers.EventDecodingTools._
  import membersSynchronizer._
  import tsReadinessChecker._

  protected override type Event = projects.Slug

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = _.event.getProjectSlug,
      process = (tsWriteLock: Lock[F, projects.Slug]).surround[Unit](synchronizeMembers(_)),
      precondition = verifyTSReady,
      onRelease = subscriptionMechanism.renewSubscription().some
    )
}

private object EventHandler {

  import eu.timepit.refined.auto._

  def apply[F[_]: Async: Network: ReProvisioningStatus: GitLabClient: MetricsRegistry: SparqlQueryTimeRecorder: Logger](
      subscriptionMechanism: SubscriptionMechanism[F],
      tsWriteLock:           TsWriteLock[F],
      projectSparqlClient:   ProjectSparqlClient[F],
      config:                Config
  ): F[consumers.EventHandler[F]] = for {
    tsReadinessChecker  <- TSReadinessForEventsChecker[F](config)
    membersSynchronizer <- MembersSynchronizer[F](projectSparqlClient)
    processExecutor     <- ProcessExecutor.concurrent(processesCount = 1)
  } yield new EventHandler[F](
    categoryName,
    tsReadinessChecker,
    membersSynchronizer,
    subscriptionMechanism,
    processExecutor,
    tsWriteLock
  )
}
