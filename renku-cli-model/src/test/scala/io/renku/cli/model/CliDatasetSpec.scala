package io.renku.cli.model

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.DatasetGenerators
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliDatasetSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with DiffShouldMatcher {

  implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.sample.get

  val datasetGen = DatasetGenerators.datasetGen

  "decode/encode" should {
    "be compatible" in {
      forAll(datasetGen) { cliDataset =>
        val jsonLD = cliDataset.asJsonLD
        val back = jsonLD.cursor
          .as[CliDataset]
          .fold(throw _, identity)

        back shouldMatchTo cliDataset
      }
    }
  }
}
