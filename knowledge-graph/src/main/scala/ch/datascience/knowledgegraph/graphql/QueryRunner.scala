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

package ch.datascience.knowledgegraph.graphql

import cats.effect.Async
import io.circe.Json
import sangria.execution.Executor
import sangria.marshalling.InputUnmarshaller._
import sangria.marshalling.circe._
import sangria.schema.Schema

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class QueryRunner[Interpretation[_]: Async, +QueryContext](
    schema:                  Schema[QueryContext, Unit],
    repository:              QueryContext
)(implicit executionContext: ExecutionContext) {

  def run(userQuery: UserQuery): Interpretation[Json] = Async[Interpretation].async { callback =>
    Executor.execute(schema, userQuery.query, repository, variables = mapVars(userQuery.variables)).onComplete {
      case Success(result)    => callback(Right(result))
      case Failure(exception) => callback(Left(exception))
    }
  }
}
