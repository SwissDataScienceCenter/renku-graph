package ch.datascience.tokenrepository.repository

import cats.effect.{ContextShift, IO}
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private abstract class TransactorProvider[Interpretation[_]] {
  def transactor: Aux[Interpretation, Unit]
}

private object IODBTransactorProvider extends TransactorProvider[IO] {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  lazy val transactor: Aux[IO, Unit] = Transactor.fromDriverManager[IO](
    driver = "org.postgresql.Driver",
    url    = "jdbc:postgresql:projects_tokens",
    user   = "tokenstorage",
    pass   = ""
  )
}
