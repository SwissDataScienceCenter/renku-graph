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

package ch.datascience.triplesgenerator.events.categories.membersync

import cats.data.EitherT.fromEither
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.events.consumers.EventSchedulingResult.Accepted
import ch.datascience.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess}
import ch.datascience.events.{EventRequestContent, consumers}
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[events] class EventHandler[Interpretation[_]: MonadThrow: ContextShift: Concurrent](
    override val categoryName: CategoryName,
    membersSynchronizer:       MembersSynchronizer[Interpretation],
    logger:                    Logger[Interpretation]
) extends consumers.EventHandlerWithProcessLimiter[Interpretation](ConcurrentProcessesLimiter.withoutLimit) {

  import ch.datascience.graph.model.projects
  import membersSynchronizer._

  override def createHandlingProcess(
      request: EventRequestContent
  ): Interpretation[EventHandlingProcess[Interpretation]] =
    EventHandlingProcess[Interpretation](startSynchronizingMember(request))

  private def startSynchronizingMember(request: EventRequestContent) = for {
    projectPath <- fromEither[Interpretation](request.event.getProjectPath)
    result <- (ContextShift[Interpretation].shift *> Concurrent[Interpretation]
                .start(synchronizeMembers(projectPath))).toRightT
                .map(_ => Accepted)
                .semiflatTap(logger.log(projectPath))
                .leftSemiflatTap(logger.log(projectPath))
  } yield result

  private implicit lazy val eventInfoToString: Show[projects.Path] = Show.show(path => s"projectPath = $path")
}

private[events] object EventHandler {
  def apply(gitLabThrottler: Throttler[IO, GitLab], timeRecorder: SparqlQueryTimeRecorder[IO])(implicit
      executionContext:      ExecutionContext,
      contextShift:          ContextShift[IO],
      timer:                 Timer[IO],
      logger:                Logger[IO]
  ): IO[EventHandler[IO]] = for {
    membersSynchronizer <- MembersSynchronizer(gitLabThrottler, timeRecorder)
  } yield new EventHandler[IO](categoryName, membersSynchronizer, logger)
}
