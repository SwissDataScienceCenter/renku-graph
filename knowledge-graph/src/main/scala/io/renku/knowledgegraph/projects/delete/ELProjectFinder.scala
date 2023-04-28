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

package io.renku.knowledgegraph.projects.delete

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.events.consumers.Project
import io.renku.graph.eventlog.EventLogClient
import io.renku.graph.eventlog.EventLogClient.{Result, SearchCriteria}
import io.renku.graph.model.events.EventInfo
import io.renku.graph.model.projects
import io.renku.http.rest.paging.model.PerPage
import org.typelevel.log4cats.Logger

private trait ELProjectFinder[F[_]] {
  def findProject(path: projects.Path): F[Option[Project]]
}

private object ELProjectFinder {
  def apply[F[_]: Async: Logger]: F[ELProjectFinder[F]] =
    EventLogClient[F].map(new ELProjectFinderImpl[F](_))
}

private class ELProjectFinderImpl[F[_]: MonadThrow](elClient: EventLogClient[F]) extends ELProjectFinder[F] {

  override def findProject(path: projects.Path): F[Option[Project]] =
    elClient
      .getEvents(SearchCriteria.forProject(path).withPerPage(PerPage(1)))
      .flatMap(toResult)

  private def toResult: Result[List[EventInfo]] => F[Option[Project]] =
    _.toEither.fold(
      _.raiseError[F, Option[Project]],
      _.headOption.map(ei => Project(ei.project.id, ei.project.path)).pure[F]
    )
}
