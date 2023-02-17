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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.v10migration

import cats.effect.Async
import cats.syntax.all._
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

import java.time.Instant

private trait MigrationDateFinder[F[_]] {
  def findMigrationStartDate: F[Instant]
}

private object MigrationDateFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[MigrationDateFinder[F]] =
    new MigrationDateFinderImpl[F].pure[F].widen
}

private class MigrationDateFinderImpl[F[_]] extends MigrationDateFinder[F] {

  override def findMigrationStartDate: F[Instant] = ???
}
