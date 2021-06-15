package ch.datascience.commiteventservice.events.categories.globalcommitsync

import ch.datascience.commiteventservice.events.categories.globalcommitsync.Generators.globalCommitSyncEvents
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import ch.datascience.generators.Generators.Implicits._

class GlobalCommitSyncEventSpec extends AnyWordSpec with should.Matchers  {

  "show" should {

    "print out the event id, project id, and path along with the last sync date" in {
      val event = globalCommitSyncEvents.generateOne
      event.show shouldBe         s"projectId = ${event.project.id}, " +
        s"projectPath = ${event.project.path}, " +
        s"lastSynced = ${event.lastSynced}"
    }
  }
}
