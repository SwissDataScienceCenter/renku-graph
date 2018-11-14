package ch.datascience.webhookservice

import ch.datascience.tinytypes.StringValue
import ch.datascience.tinytypes.constraints.NonBlank

case class FilePath(value: String) extends StringValue with NonBlank {
  verify(!value.startsWith("/"), s"'$value' is not a valid $typeName")
}

trait GitSha extends StringValue with NonBlank {

  import GitSha.validationRegex

  verify(value.matches(validationRegex), s"'$value' is not a valid Git sha")
}

object GitSha {
  private val validationRegex: String = "[0-9a-f]{5,40}"
}