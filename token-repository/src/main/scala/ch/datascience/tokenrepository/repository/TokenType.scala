package ch.datascience.tokenrepository.repository

import ch.datascience.tinytypes.TinyType

private sealed trait TokenType extends TinyType[String] with Product with Serializable

private object TokenType {

  final case object Personal extends TokenType {
    override val value: String = "PERSONAL"
  }

  final case object OAuth extends TokenType {
    override val value: String = "OAUTH"
  }
}
