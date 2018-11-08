package ch.datascience.tinytypes.constraints

import ch.datascience.tinytypes.StringValue

trait NonBlank {
  self: StringValue =>
  verify(value.trim.nonEmpty, s"$typeName cannot be blank")
}
