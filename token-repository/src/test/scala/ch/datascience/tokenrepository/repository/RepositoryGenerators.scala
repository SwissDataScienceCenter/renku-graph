package ch.datascience.tokenrepository.repository

import org.scalacheck.Gen

private object RepositoryGenerators {
  implicit val tokenTypes: Gen[TokenType] = Gen.oneOf(TokenType.OAuth, TokenType.Personal)
}
