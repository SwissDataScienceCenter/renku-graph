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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest

import ConditionedMigration._
import Migration.Name
import cats.data.EitherT
import cats.data.EitherT._
import cats.syntax.all._
import cats.{MonadThrow, Show}
import io.renku.tinytypes.constraints.NonBlank
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import org.typelevel.log4cats.Logger

private trait Migration[F[_]] {
  def name:  Name
  def run(): EitherT[F, ProcessingRecoverableError, Unit]
}

private object Migration {
  final class Name private (val value: String) extends AnyVal with StringTinyType
  object Name                                  extends TinyTypeFactory[Name](new Name(_)) with NonBlank
}

private abstract class ConditionedMigration[F[_]: MonadThrow: Logger] extends Migration[F] {

  protected def required:  F[MigrationRequired]
  protected def migrate(): EitherT[F, ProcessingRecoverableError, Unit]

  final def run(): EitherT[F, ProcessingRecoverableError, Unit] = right[ProcessingRecoverableError](required) >>= {
    case r: MigrationRequired.No =>
      right(Logger[F].info(show"$categoryName: $name $r"))
    case r: MigrationRequired.Yes =>
      right[ProcessingRecoverableError](Logger[F].info(show"$categoryName: $name $r")) >> migrate()
  }
}

private object ConditionedMigration {

  sealed trait MigrationRequired extends Product with Serializable { val message: String }
  object MigrationRequired {
    final case class Yes(message: String) extends MigrationRequired
    final case class No(message: String)  extends MigrationRequired

    implicit val show: Show[MigrationRequired] = Show.show {
      case MigrationRequired.Yes(message) => s"required as $message"
      case MigrationRequired.No(message)  => s"not required as $message"
    }
  }
}
