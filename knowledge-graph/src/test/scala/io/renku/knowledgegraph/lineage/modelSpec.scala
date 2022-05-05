package io.renku.knowledgegraph.lineage

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.relativePaths
import io.renku.knowledgegraph.lineage.model.Node.Location
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class modelSpec extends AnyWordSpec with should.Matchers {

  "Location.unapply" should {
    "correctly extract the arguments" in {
      val path     = relativePaths().generateOne
      val location = Location(path)
      Location.unapply(location) shouldBe Some(path)
    }
  }
}
