package io.renku.entities.searchgraphs

import io.renku.entities.searchgraphs.SearchInfo.DateModified
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.datasetCreatedDates
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SearchInfoSpec extends AnyWordSpec with should.Matchers {

  "DateModified.apply(DateCreated)" should {

    "instantiate DateModified from a Dataset DateCreated" in {
      val created = datasetCreatedDates().generateOne
      DateModified(created).value shouldBe created.instant
    }
  }
}
