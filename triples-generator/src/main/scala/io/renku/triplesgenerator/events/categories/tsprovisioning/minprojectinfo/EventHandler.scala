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

package io.renku.triplesgenerator.events.categories.tsprovisioning.minprojectinfo

import cats.data.EitherT.fromEither
import cats.effect.{Async, Concurrent, Spawn}
import cats.syntax.all._
import cats.{NonEmptyParallel, Parallel}
import io.renku.events.consumers.EventSchedulingResult.Accepted
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess}
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

private[events] class EventHandler[F[_]: Concurrent: Logger](
    override val categoryName:  CategoryName,
    concurrentProcessesLimiter: ConcurrentProcessesLimiter[F],
    eventProcessor:             EventProcessor[F]
) extends consumers.EventHandlerWithProcessLimiter[F](concurrentProcessesLimiter) {

  import eventProcessor._

  override def createHandlingProcess(request: EventRequestContent) =
    EventHandlingProcess[F](startProcessingEvent(request))

  private def startProcessingEvent(request: EventRequestContent) = for {
    project <- fromEither(request.event.getProject)
    result <- Spawn[F]
                .start(process(MinProjectInfoEvent(project)))
                .toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(project))
                .leftSemiflatTap(Logger[F].log(project))
  } yield result
}

private[events] object EventHandler {
  import eu.timepit.refined.auto._

  def apply[F[
      _
  ]: Async: NonEmptyParallel: Parallel: GitLabClient: AccessTokenFinder: MetricsRegistry: Logger: SparqlQueryTimeRecorder]
      : F[EventHandler[F]] = for {
    concurrentProcessesLimiter <- ConcurrentProcessesLimiter(processesCount = 2)
    eventProcessor             <- EventProcessor[F]
  } yield new EventHandler[F](categoryName, concurrentProcessesLimiter, eventProcessor)
}