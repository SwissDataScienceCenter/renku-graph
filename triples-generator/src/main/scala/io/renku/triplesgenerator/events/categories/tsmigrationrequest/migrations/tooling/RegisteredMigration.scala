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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations
package tooling

import cats.MonadThrow
import cats.data.EitherT
import cats.syntax.all._
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.ConditionedMigration.MigrationRequired
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.{ConditionedMigration, Migration}
import org.typelevel.log4cats.Logger

private[migrations] abstract class RegisteredMigration[F[_]: MonadThrow: Logger](
    val name:          Migration.Name,
    executionRegister: MigrationExecutionRegister[F],
    recoveryStrategy:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends ConditionedMigration[F] {
  import recoveryStrategy._

  protected[tooling] override def required: EitherT[F, ProcessingRecoverableError, MigrationRequired] = EitherT {
    executionRegister
      .findExecution(name)
      .map {
        case Some(serviceVersion) => MigrationRequired.No(s"was executed on $serviceVersion")
        case None                 => MigrationRequired.Yes("was not executed yet")
      }
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, MigrationRequired])
  }

  protected[tooling] override def postMigration(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    executionRegister
      .registerExecution(name)
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }
}
