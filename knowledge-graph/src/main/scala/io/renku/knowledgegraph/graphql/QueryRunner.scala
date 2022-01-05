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

package io.renku.knowledgegraph.graphql

import cats.MonadThrow
import cats.effect.Async
import io.circe.Json
import sangria.execution.Executor
import sangria.marshalling.InputUnmarshaller._
import sangria.marshalling.circe._
import sangria.schema.Schema

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait QueryRunner[F[_], T <: QueryContext[T]] {
  val schema:     Schema[T, Unit]
  val repository: T

  def run(userQuery: UserQuery, updateCtxParameters: repository.Params): F[Json]
}

object QueryRunner {
  def apply[F[_]: Async, T <: QueryContext[T]](
      schema:                  Schema[T, Unit],
      repository:              T
  )(implicit executionContext: ExecutionContext): F[QueryRunner[F, T]] =
    MonadThrow[F].catchNonFatal(new QueryRunnerImpl(schema, repository))
}

class QueryRunnerImpl[F[_]: Async, T <: QueryContext[T]](
    override val schema:     Schema[T, Unit],
    override val repository: T
)(implicit executionContext: ExecutionContext)
    extends QueryRunner[F, T] {

  def run(userQuery: UserQuery, updateCtxParameters: repository.Params): F[Json] =
    Async[F].async_[Json] { callback =>
      Executor
        .execute(schema,
                 userQuery.query,
                 repository.updateContext(updateCtxParameters),
                 variables = mapVars(userQuery.variables)
        )
        .onComplete {
          case Success(result)    => callback(Right(result))
          case Failure(exception) => callback(Left(exception))
        }
    }
}
