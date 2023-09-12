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

package io.renku.triplesgenerator.projects.create

import cats.effect.Async
import cats.effect.kernel.MonadCancelThrow
import cats.syntax.all._
import io.renku.triplesgenerator.TgLockDB.TsWriteLock
import io.renku.triplesgenerator.api.NewProject
import io.renku.triplesgenerator.projects.ProjectExistenceChecker
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private trait ProjectCreator[F[_]] {
  def createProject(project: NewProject): F[Unit]
}

private object ProjectCreator {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](tsWriteLock: TsWriteLock[F]): F[ProjectCreator[F]] =
    for {
      connectionConfig <- ProjectsConnectionConfig[F]()
      projectExistenceChecker = ProjectExistenceChecker[F](connectionConfig)
    } yield new ProjectCreatorImpl[F](projectExistenceChecker, tsWriteLock)
}

private class ProjectCreatorImpl[F[_]: MonadCancelThrow](projectExistenceChecker: ProjectExistenceChecker[F],
                                                         tsWriteLock: TsWriteLock[F]
) extends ProjectCreator[F] {

  override def createProject(project: NewProject): F[Unit] =
    tsWriteLock(project.slug).surround(create(project))

  private def create(project: NewProject): F[Unit] =
    projectExistenceChecker.checkExists(project.slug).void
}
