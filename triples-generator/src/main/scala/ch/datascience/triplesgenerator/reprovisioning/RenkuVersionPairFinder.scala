package ch.datascience.triplesgenerator.reprovisioning

import cats.effect.IO
import ch.datascience.triplesgenerator.models.RenkuVersionPair

// See TriplesVersionFinder
trait RenkuVersionPairFinder[Interpretation[_]] {

  def find(): Interpretation[RenkuVersionPair]

}

private class RenkuVersionPairFinderImpl[Interpretation[_]] extends RenkuVersionPairFinder[Interpretation] {
  override def find(): Interpretation[RenkuVersionPair] = ???
}

object IORenkuVersionPairFinder {
  def apply(): RenkuVersionPairFinder[IO] = new RenkuVersionPairFinderImpl
}
