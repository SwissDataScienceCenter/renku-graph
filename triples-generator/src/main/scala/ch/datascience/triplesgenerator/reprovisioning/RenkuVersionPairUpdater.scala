package ch.datascience.triplesgenerator.reprovisioning

import cats.effect.IO
import ch.datascience.triplesgenerator.models.RenkuVersionPair

trait RenkuVersionPairUpdater[Interpretation[_]] {

  def update(): Interpretation[Unit]

}

private class RenkuVersionPairUpdaterImpl[Interpretation[_]](versionCompatibilityPairs: List[RenkuVersionPair])
    extends RenkuVersionPairUpdater[Interpretation] {
  override def update(): Interpretation[Unit] = ???
}

object IORenkuVersionPairUpdater {

  // TODO: to think about: will this duplicate some of the logic from the judge in determining what the new pair should be?
  def apply(versionCompatibilityPairs: List[RenkuVersionPair]): RenkuVersionPairUpdater[IO] =
    new RenkuVersionPairUpdaterImpl(
      versionCompatibilityPairs
    )
}
