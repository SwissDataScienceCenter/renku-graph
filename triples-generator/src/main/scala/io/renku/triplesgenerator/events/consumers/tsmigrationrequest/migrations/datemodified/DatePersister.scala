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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.datemodified

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait DatePersister[F[_]] {
  def persistDateModified(projectInfo: ProjectInfo): F[Unit]
}

private object DatePersister {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): DatePersister[F] = new DatePersisterImpl[F](TSClient[F](connectionConfig))
}

private class DatePersisterImpl[F[_]: MonadThrow](tsClient: TSClient[F]) extends DatePersister[F] {

  override def persistDateModified(projectInfo: ProjectInfo): F[Unit] = println(tsClient).pure[F]
}
