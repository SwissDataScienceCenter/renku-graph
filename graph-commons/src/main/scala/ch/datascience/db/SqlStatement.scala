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

package ch.datascience.db

import cats.data.Kleisli
import cats.effect.Bracket
import cats.syntax.all._
import cats.{Functor, Monad}
import ch.datascience.db.SqlStatement.Name
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import skunk.data.Completion
import skunk.{Query, _}

final case class SqlStatement[Interpretation[_], ResultType](
    queryExecution: Kleisli[Interpretation, Session[Interpretation], ResultType],
    name:           Name
) {
  def flatMapResult[O](f: ResultType => Interpretation[O])(implicit
      monad:              Monad[Interpretation]
  ): SqlStatement[Interpretation, O] =
    copy(queryExecution = queryExecution.flatMapF(f))

  def mapResult[O](f: ResultType => O)(implicit functor: Functor[Interpretation]): SqlStatement[Interpretation, O] =
    copy(queryExecution = queryExecution.map(f))

  def void(implicit functor: Functor[Interpretation]): SqlStatement[Interpretation, Unit] =
    copy(queryExecution = queryExecution.void)
}

object SqlStatement {
  type Name = String Refined NonEmpty

  def apply[Interpretation[_]: Bracket[*[_], Throwable]](name: Name): QueryBuilder[Interpretation] =
    new QueryBuilder[Interpretation](name)

  class QueryBuilder[Interpretation[_]: Bracket[*[_], Throwable]](val name: Name) {
    def select[In, Out](query: Query[In, Out]): SelectBuilder[Interpretation, In, Out] =
      SelectBuilder[Interpretation, In, Out](query, name)

    def command[In](command: skunk.Command[In]): CommandBuilder[Interpretation, In] = CommandBuilder(command, name)
  }

  case class SelectBuilder[Interpretation[_]: Bracket[*[_], Throwable], In, Out](query: Query[In, Out], name: Name) {
    def arguments(args: In): Select[Interpretation, In, Out] = Select(query, name, args = args)
  }

  case class CommandBuilder[Interpretation[_]: Bracket[*[_], Throwable], In](command: skunk.Command[In], name: Name) {
    def arguments(args: In): Command[Interpretation, In] = Command(command, name, args = args)
  }

  case class Select[Interpretation[_]: Bracket[*[_], Throwable], In, Out](query: Query[In, Out], name: Name, args: In) {
    def build[F[_]](
        queryExecution: PreparedQuery[Interpretation, In, Out] => In => Interpretation[F[Out]]
    ): SqlStatement[Interpretation, F[Out]] = SqlStatement[Interpretation, F[Out]](
      Kleisli(session => session.prepare(query).use(queryExecution(_)(args))),
      name
    )
  }

  case class Command[Interpretation[_]: Bracket[*[_], Throwable], In](
      command: skunk.Command[In],
      name:    Name,
      args:    In
  ) {
    def build: SqlStatement[Interpretation, Completion] =
      SqlStatement[Interpretation, Completion](Kleisli(session => session.prepare(command).use(_.execute(args))), name)
  }
}
