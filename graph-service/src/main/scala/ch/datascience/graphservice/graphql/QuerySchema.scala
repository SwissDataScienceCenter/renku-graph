package ch.datascience.graphservice.graphql

import sangria.schema._

import scala.language.higherKinds

object QuerySchema {

  def apply[Interpretation[_]](
      fields: List[Field[QueryContext[Interpretation], Unit]]
  ): Schema[QueryContext[Interpretation], Unit] = Schema {
    ObjectType(
      name   = "Query",
      fields = fields
    )
  }
}
