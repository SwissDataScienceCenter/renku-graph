package io.renku.eventlog.init

import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventStatusRenamerImplSpec extends AnyWordSpec with DbInitSpec with should.Matchers {
  protected override lazy val migrationsToRun: List[Migration] = List(
    eventLogTableCreator,
    projectPathAdder,
    batchDateAdder,
    latestEventDatesViewRemover,
    projectTableCreator,
    projectPathRemover,
    eventLogTableRenamer
  )

  "run" should {
    "" in {
      fail()
    }
  }
}
