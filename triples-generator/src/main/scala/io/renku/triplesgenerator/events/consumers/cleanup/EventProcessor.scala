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

package io.renku.triplesgenerator.events.consumers.cleanup

import cats.effect.Async
import cats.syntax.all._
import io.renku.events.consumers.Project
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.events.consumers.EventStatusUpdater
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait EventProcessor[F[_]] {
  def process(project: Project): F[Unit]
}

private class EventProcessorImpl[F[_]: Async: Logger](defaultGraphCleaner: defaultgraph.TSCleaner[F],
                                                      namedGraphsCleaner: namedgraphs.TSCleaner[F],
                                                      statusUpdater:      EventStatusUpdater[F]
) extends EventProcessor[F] {

  override def process(project: Project): F[Unit] = {
    defaultGraphCleaner.removeTriples(project.path) >>
      namedGraphsCleaner.removeTriples(project.path) >>
      statusUpdater.projectToNew(project)
  }.recoverWith(logError(project))

  private def logError(project: Project): PartialFunction[Throwable, F[Unit]] = { case NonFatal(error) =>
    Logger[F].error(error)(show"$categoryName: $project - triples removal failed ${error.getMessage}")
  }
}

private object EventProcessor {
  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder]: F[EventProcessor[F]] = for {
    eventStatusUpdater  <- EventStatusUpdater(categoryName)
    defaultGraphCleaner <- defaultgraph.TSCleaner[F]()
    namedGraphsCleaner  <- namedgraphs.TSCleaner[F]()
  } yield new EventProcessorImpl[F](defaultGraphCleaner, namedGraphsCleaner, eventStatusUpdater)
}
