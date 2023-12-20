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

package io.renku.eventlog.events.consumers.projectsync

import cats.effect._
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.{consumers, CategoryName}
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.ProcessExecutor
import io.renku.http.client.GitLabClient
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: MonadCancelThrow: Logger](projectInfoSynchronizer: ProjectInfoSynchronizer[F],
                                                           subscriptionMechanism:     SubscriptionMechanism[F],
                                                           processExecutor:           ProcessExecutor[F],
                                                           override val categoryName: CategoryName = categoryName
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  protected override type Event = ProjectSyncEvent

  import io.renku.events.consumers.EventDecodingTools._
  import projectInfoSynchronizer._

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = _.event.getProject.map(p => ProjectSyncEvent(p.id, p.slug)),
      process = ev => Logger[F].info(show"$categoryName: $ev accepted") >> syncProjectInfo(ev),
      onRelease = subscriptionMechanism.renewSubscription()
    )
}

private object EventHandler {
  import com.typesafe.config.{Config, ConfigFactory}
  import eu.timepit.refined.api.Refined
  import eu.timepit.refined.numeric.Positive
  import eu.timepit.refined.pureconfig._
  import io.renku.config.ConfigLoader.find

  def apply[F[_]: Async: GitLabClient: SessionResource: Logger: MetricsRegistry: QueriesExecutionTimes](
      subscriptionMechanism: SubscriptionMechanism[F],
      config:                Config = ConfigFactory.load()
  ): F[consumers.EventHandler[F]] = for {
    projectInfoSynchronizer    <- ProjectInfoSynchronizer[F]
    processesCount             <- find[F, Int Refined Positive]("project-sync-max-concurrent-processes", config)
    concurrentProcessesLimiter <- ProcessExecutor.concurrent[F](processesCount)
  } yield new EventHandler[F](projectInfoSynchronizer, subscriptionMechanism, concurrentProcessesLimiter)
}
