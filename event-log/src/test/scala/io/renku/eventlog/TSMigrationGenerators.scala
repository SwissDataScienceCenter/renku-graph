package io.renku.eventlog

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import org.scalacheck.Gen

object TSMigrationGenerators {
  implicit val changeDates: Gen[ChangeDate] = timestampsNotInTheFuture.toGeneratorOf(ChangeDate)
}
