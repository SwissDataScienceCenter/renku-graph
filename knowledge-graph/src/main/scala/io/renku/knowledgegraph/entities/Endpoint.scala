package io.renku.knowledgegraph.entities

import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}
import io.renku.tinytypes.constraints.NonBlank

object Endpoint {

  final class QueryParam private (val value: String) extends AnyVal with StringTinyType
  object QueryParam extends TinyTypeFactory[QueryParam](new QueryParam(_)) with NonBlank
}
