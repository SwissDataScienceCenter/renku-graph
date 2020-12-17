package ch.datascience.triplesgenerator.init

import cats.MonadError
import cats.data.NonEmptyList
import cats.effect.IO
import ch.datascience.graph.model.CliVersion
import ch.datascience.triplesgenerator.models.RenkuVersionPair
import cats.syntax.all._

trait CliVersionCompatibilityVerifier[Interpretation[_]] {

  def run(): Interpretation[Unit]

}

private class CliVersionCompatibilityVerifierImpl[Interpretation[_]](cliVersion:        CliVersion,
                                                                     renkuVersionPairs: NonEmptyList[RenkuVersionPair]
)(implicit ME:                                                                          MonadError[Interpretation, Throwable])
    extends CliVersionCompatibilityVerifier[Interpretation] {
  override def run(): Interpretation[Unit] =
    if (cliVersion != renkuVersionPairs.head.cliVersion)
      ME.raiseError(
        new IllegalStateException(
          s"Incompatible versions. cliVersion: $cliVersion versionPairs: ${renkuVersionPairs.head.cliVersion}"
        )
      )
    else ().pure[Interpretation]
}

object IOCliVersionCompatibilityChecker {
  def apply(cliVersion:        CliVersion,
            renkuVersionPairs: NonEmptyList[RenkuVersionPair]
  ): IO[CliVersionCompatibilityVerifier[IO]] = IO(
    new CliVersionCompatibilityVerifierImpl[IO](cliVersion, renkuVersionPairs)
  )
}
