package ch.datascience.triplesgenerator.reprovisioning

import cats.data.NonEmptyList
import cats.effect.IO
import ch.datascience.triplesgenerator.models.RenkuVersionPair

trait ReprovisionJudge[Interpretation[_]] {

  def isReprovisioningNeeded(currentVersionPair:        RenkuVersionPair,
                             versionCompatibilityPairs: NonEmptyList[RenkuVersionPair]
  ): Interpretation[Boolean]

}

private class ReprovisionJudgeImpl[Interpretation[_]] extends ReprovisionJudge[Interpretation] {

  override def isReprovisioningNeeded(currentVersionPair:        RenkuVersionPair,
                                      versionCompatibilityPairs: NonEmptyList[RenkuVersionPair]
  ): Interpretation[Boolean] = ???
}

object IOReprovisionJudge {

  def apply(): ReprovisionJudge[IO] = new ReprovisionJudgeImpl[IO]
}
