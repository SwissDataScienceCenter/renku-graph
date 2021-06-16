package io.renku.eventlog.events.categories.creation

import ch.datascience.graph.model.events.CompoundEventId
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import ch.datascience.generators.Generators.Implicits._
import org.scalatest.matchers.should
import Generators._

class EventSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "compoundEventId" should {

    "create a CompoundEventId from the event's id and project id" in {
      forAll { event: Event =>
        event.compoundEventId shouldBe CompoundEventId(event.id, event.project.id)
      }
    }
  }
}
