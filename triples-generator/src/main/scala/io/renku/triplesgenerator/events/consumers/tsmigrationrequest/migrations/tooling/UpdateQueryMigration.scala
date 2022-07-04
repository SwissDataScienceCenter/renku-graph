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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations.tooling

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.rdfstore.{SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import org.typelevel.log4cats.Logger

private[migrations] class UpdateQueryMigration[F[_]: MonadThrow](
    val name:          Migration.Name,
    updateQuery:       SparqlQuery,
    updateQueryRunner: UpdateQueryRunner[F],
    recoveryStrategy:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends Migration[F] {
  import recoveryStrategy._

  override def run(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    updateQueryRunner
      .run(updateQuery)
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }
}

private[migrations] object UpdateQueryMigration {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      name:        Migration.Name,
      updateQuery: SparqlQuery
  ): F[UpdateQueryMigration[F]] = for {
    queryRunner <- UpdateQueryRunner[F]
  } yield new UpdateQueryMigration[F](name, updateQuery, queryRunner)
}
