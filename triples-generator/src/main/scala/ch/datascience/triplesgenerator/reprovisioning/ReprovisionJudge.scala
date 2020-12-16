package ch.datascience.triplesgenerator.reprovisioning

import cats.data.NonEmptyList
import cats.effect.IO
import ch.datascience.graph.model.projects._
import ch.datascience.graph.model.{CliVersion, projects}
import ch.datascience.triplesgenerator.models.RenkuVersionPair

trait ReprovisionJudge {

  def isReprovisioningNeeded(currentVersionPair:        RenkuVersionPair,
                             versionCompatibilityPairs: NonEmptyList[RenkuVersionPair]
  ): Boolean

}

private class ReprovisionJudgeImpl extends ReprovisionJudge {

  override def isReprovisioningNeeded(currentVersionPair:        RenkuVersionPair,
                                      versionCompatibilityPairs: NonEmptyList[RenkuVersionPair]
  ): Boolean =
    `is current schema version different from latest`(currentVersionPair.schemaVersion,
                                                      versionCompatibilityPairs.head.schemaVersion
    ) || `are latest schema version same but cli versions different`(versionCompatibilityPairs.toList,
                                                                     currentVersionPair.cliVersion
    )

  private def `is current schema version different from latest`(current: SchemaVersion, latest: SchemaVersion) =
    current != latest

  private def `are latest schema version same but cli versions different`(
      versionCompatibilityPairs: List[RenkuVersionPair],
      currentCliVersion:         CliVersion
  ) =
    versionCompatibilityPairs match {
      case List(RenkuVersionPair(latestCliVersion, latestSchemaVersion), RenkuVersionPair(_, oldSchemaVersion))
          if latestSchemaVersion == oldSchemaVersion =>
        latestCliVersion != currentCliVersion
      case _ => false
    }
}
