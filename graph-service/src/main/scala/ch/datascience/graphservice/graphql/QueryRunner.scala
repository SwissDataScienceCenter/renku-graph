package ch.datascience.graphservice.graphql

import cats.effect.Async
import io.circe.Json
import sangria.execution.Executor
import sangria.marshalling.circe._
import sangria.schema.Schema

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.{Failure, Success}

class QueryRunner[Interpretation[_]: Async, +QuerySchema](
    schema:                  Schema[QuerySchema, Unit],
    repository:              QuerySchema
)(implicit executionContext: ExecutionContext) {

  def run(userQuery: UserQuery): Interpretation[Json] = Async[Interpretation].async { callback =>
    Executor.execute(schema, userQuery.query, repository).onComplete {
      case Success(result)    => callback(Right(result))
      case Failure(exception) => callback(Left(exception))
    }
  }
}
