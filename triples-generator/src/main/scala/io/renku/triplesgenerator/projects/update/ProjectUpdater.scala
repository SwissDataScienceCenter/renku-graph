/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.projects.update

import ProjectUpdater.Result
import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import io.renku.graph.model.projects
import io.renku.lock.Lock
import io.renku.lock.syntax._
import io.renku.triplesgenerator.TgDB.TsWriteLock
import io.renku.triplesgenerator.api.ProjectUpdates
import io.renku.triplesgenerator.projects.ProjectExistenceChecker
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait ProjectUpdater[F[_]] {
  def updateProject(slug: projects.Slug, updates: ProjectUpdates): F[Result]
}

private object ProjectUpdater {

  sealed trait Result {
    lazy val widen: Result = this
  }
  object Result {
    final case object Updated   extends Result
    final case object NotExists extends Result
  }

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](tsWriteLock: TsWriteLock[F]): F[ProjectUpdater[F]] = for {
    connectionConfig <- ProjectsConnectionConfig[F]()
    projectExistenceChecker = ProjectExistenceChecker[F](connectionConfig)
    updateQueriesCalculator <- UpdateQueriesCalculator[F]()
  } yield new ProjectUpdaterImpl[F](projectExistenceChecker,
                                    updateQueriesCalculator,
                                    TSClient(connectionConfig),
                                    tsWriteLock
  )
}

private class ProjectUpdaterImpl[F[_]: MonadCancelThrow](projectExistenceChecker: ProjectExistenceChecker[F],
                                                         updateQueriesCalculator: UpdateQueriesCalculator[F],
                                                         tsClient:                TSClient[F],
                                                         tsWriteLock:             TsWriteLock[F]
) extends ProjectUpdater[F] {

  import updateQueriesCalculator.calculateUpdateQueries

  override def updateProject(slug: projects.Slug, updates: ProjectUpdates): F[Result] =
    (tsWriteLock: Lock[F, projects.Slug]).surround(update(updates)).apply(slug)

  private def update(updates: ProjectUpdates): projects.Slug => F[Result] = { slug =>
    projectExistenceChecker.checkExists(slug) >>= {
      case false =>
        Result.NotExists.widen.pure[F]
      case true =>
        calculateUpdateQueries(slug, updates)
          .flatMap(_.traverse_(tsClient.updateWithNoResult))
          .as(Result.Updated.widen)
    }
  }
}
