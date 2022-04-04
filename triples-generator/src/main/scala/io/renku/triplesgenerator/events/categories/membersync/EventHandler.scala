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

package io.renku.triplesgenerator.events.categories.membersync

import cats.Show
import cats.data.EitherT.fromEither
import cats.effect.{Async, Concurrent, Spawn}
import cats.syntax.all._
import io.renku.events.consumers.EventSchedulingResult.Accepted
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess}
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.http.client.GitLabClient
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

private[events] class EventHandler[F[_]: Concurrent: Logger](
    override val categoryName: CategoryName,
    membersSynchronizer:       MembersSynchronizer[F]
) extends consumers.EventHandlerWithProcessLimiter[F](ConcurrentProcessesLimiter.withoutLimit) {

  import io.renku.graph.model.projects
  import membersSynchronizer._

  override def createHandlingProcess(
      request: EventRequestContent
  ): F[EventHandlingProcess[F]] =
    EventHandlingProcess[F](startSynchronizingMember(request))

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
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](gitLabClient: GitLabClient[F]): F[EventHandler[F]] = for {
    membersSynchronizer <- MembersSynchronizer[F](gitLabClient)
  } yield new EventHandler[F](categoryName, membersSynchronizer)
}
