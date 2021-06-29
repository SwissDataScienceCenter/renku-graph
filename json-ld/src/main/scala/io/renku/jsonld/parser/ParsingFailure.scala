package io.renku.jsonld.parser

trait ParsingFailure extends Exception

object ParsingFailure {
  def apply(message: String): ParsingFailure = new Exception(message) with ParsingFailure
  def apply(message: String, exception: Throwable): ParsingFailure = new Exception(message, exception)
    with ParsingFailure
}
