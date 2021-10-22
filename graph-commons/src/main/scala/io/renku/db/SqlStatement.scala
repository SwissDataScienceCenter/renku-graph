/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.db

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import cats.{Functor, Monad}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.db.SqlStatement.Name
import skunk._
import skunk.data.Completion

final case class SqlStatement[F[_], ResultType](
    queryExecution: Kleisli[F, Session[F], ResultType],
    name:           Name
) {
  def flatMapResult[O](f: ResultType => F[O])(implicit
      monad:              Monad[F]
  ): SqlStatement[F, O] =
    copy(queryExecution = queryExecution.flatMapF(f))

  def mapResult[O](f: ResultType => O)(implicit functor: Functor[F]): SqlStatement[F, O] =
    copy(queryExecution = queryExecution.map(f))

  def void(implicit functor: Functor[F]): SqlStatement[F, Unit] =
    copy(queryExecution = queryExecution.void)
}

object SqlStatement {
  type Name = String Refined NonEmpty

  def apply[F[_]: MonadCancelThrow](name: Name): QueryBuilder[F] =
    new QueryBuilder[F](name)

  class QueryBuilder[F[_]: MonadCancelThrow](val name: Name) {

    def select[In, Out](query: Query[In, Out]): SelectBuilder[F, In, Out] =
      SelectBuilder[F, In, Out](query, name)

    def command[In](command: skunk.Command[In]): CommandBuilder[F, In] = CommandBuilder(command, name)
  }

  case class SelectBuilder[F[_]: MonadCancelThrow, In, Out](query: Query[In, Out], name: Name) {
    def arguments(args: In): Select[F, In, Out] = Select(query, name, args = args)
  }

  case class CommandBuilder[F[_]: MonadCancelThrow, In](command: skunk.Command[In], name: Name) {
    def arguments(args: In): Command[F, In] = Command(command, name, args = args)
  }

  case class Select[F[_]: MonadCancelThrow, In, Out](query: Query[In, Out], name: Name, args: In) {
    def build[FF[_]](
        queryExecution: PreparedQuery[F, In, Out] => In => F[FF[Out]]
    ): SqlStatement[F, FF[Out]] = SqlStatement[F, FF[Out]](
      Kleisli(session => session.prepare(query).use(queryExecution(_)(args))),
      name
    )
  }

  case class Command[F[_]: MonadCancelThrow, In](
      command: skunk.Command[In],
      name:    Name,
      args:    In
  ) {
    def build: SqlStatement[F, Completion] =
      SqlStatement[F, Completion](Kleisli(session => session.prepare(command).use(_.execute(args))), name)
  }
}
