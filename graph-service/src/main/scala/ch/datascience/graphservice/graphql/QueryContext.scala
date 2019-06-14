package ch.datascience.graphservice.graphql

import cats.effect.IO
import ch.datascience.graphservice.graphql.lineage.{IOLineageRepository, LineageRepository}

import scala.language.higherKinds

class QueryContext[Interpretation[_]](
    val lineageRepository: LineageRepository[Interpretation]
)

object IOQueryContext {
  def apply(): IO[QueryContext[IO]] =
    for {
      lineageRepository <- IOLineageRepository()
    } yield new QueryContext[IO](lineageRepository)
}
