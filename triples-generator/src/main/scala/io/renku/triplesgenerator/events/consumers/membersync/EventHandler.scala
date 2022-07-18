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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.Show
import cats.data.EitherT.fromEither
import cats.effect.{Async, Concurrent, Spawn}
import cats.syntax.all._
import io.renku.events.consumers.EventSchedulingResult.Accepted
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess}
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.triplesstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.consumers.TSReadinessForEventsChecker
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import org.typelevel.log4cats.Logger

private[events] class EventHandler[F[_]: Concurrent: Logger](
    override val categoryName: CategoryName,
    tsReadinessChecker:        TSReadinessForEventsChecker[F],
    membersSynchronizer:       MembersSynchronizer[F]
) extends consumers.EventHandlerWithProcessLimiter[F](ConcurrentProcessesLimiter.withoutLimit) {

  import io.renku.graph.model.projects
  import membersSynchronizer._
  import tsReadinessChecker._

  override def createHandlingProcess(request: EventRequestContent): F[EventHandlingProcess[F]] =
    EventHandlingProcess[F](verifyTSReady >> startSynchronizingMember(request))

  private def startSynchronizingMember(request: EventRequestContent) = for {
    projectPath <- fromEither[F](request.event.getProjectPath)
    result <- Spawn[F]
                .start(synchronizeMembers(projectPath))
                .toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(projectPath))
                .leftSemiflatTap(Logger[F].log(projectPath))
  } yield result

  private implicit lazy val eventInfoToString: Show[projects.Path] = Show.show(path => s"projectPath = $path")
}

private[events] object EventHandler {
  def apply[F[_]: Async: ReProvisioningStatus: GitLabClient: AccessTokenFinder: SparqlQueryTimeRecorder: Logger]
      : F[EventHandler[F]] = for {
    tsReadinessChecker  <- TSReadinessForEventsChecker[F]
    membersSynchronizer <- MembersSynchronizer[F]
  } yield new EventHandler[F](categoryName, tsReadinessChecker, membersSynchronizer)
}
