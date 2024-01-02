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

package io.renku.triplesgenerator.tsprovisioning
package triplesuploading

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.entities.searchgraphs
import io.renku.graph.model.datasets
import io.renku.graph.model.entities.Project
import io.renku.lock.Lock
import io.renku.triplesgenerator.errors.{ProcessingRecoverableError, RecoverableErrorsRecovery}
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private trait SearchGraphsProvisioner[F[_]] {
  def provisionSearchGraphs(project: Project): EitherT[F, ProcessingRecoverableError, Unit]
}

private object SearchGraphsProvisioner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](topSameAsLock:    Lock[F, datasets.TopmostSameAs],
                                                          connectionConfig: ProjectsConnectionConfig
  ): SearchGraphsProvisioner[F] =
    new SearchGraphsProvisionerImpl[F](searchgraphs.SearchGraphsProvisioner[F](topSameAsLock, connectionConfig))

  def default[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      topSameAsLock: Lock[F, datasets.TopmostSameAs]
  ): F[SearchGraphsProvisioner[F]] =
    ProjectsConnectionConfig[F]().map(apply(topSameAsLock, _))
}

private class SearchGraphsProvisionerImpl[F[_]: MonadThrow](
    underlyingGraphProvisioner: searchgraphs.SearchGraphsProvisioner[F],
    recoverableErrorsRecovery:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends SearchGraphsProvisioner[F] {

  import recoverableErrorsRecovery._

  override def provisionSearchGraphs(project: Project): EitherT[F, ProcessingRecoverableError, Unit] =
    EitherT {
      underlyingGraphProvisioner
        .provisionSearchGraphs(project)
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(maybeRecoverableError("Problem while provisioning Search Graphs"))
    }
}
