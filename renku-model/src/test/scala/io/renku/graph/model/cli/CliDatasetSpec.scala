package io.renku.graph.model.cli

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.graph.model.GitLabApiUrl
import io.renku.graph.model.entities.DiffInstances
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.jsonld.JsonLDDecoder
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliDatasetSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with DiffInstances
    with DiffShouldMatcher {
  val generators: CliGenerators = CliGenerators(EntitiesGenerators.renkuUrl)

  implicit val gitLabApiUrl: GitLabApiUrl = EntitiesGenerators.gitLabApiUrl

  "decode/encode" should {
    "be compatible" in {
      forAll(generators.datasetProvenanceGen) { prov =>
        implicit val decoder: JsonLDDecoder[CliDataset] =
          CliDataset.jsonLDDecoder

        val jsonLD = prov.asJsonLD
        val back = jsonLD.cursor
          .as[CliDatasetProvenance]
          .fold(throw _, identity)

        back shouldMatchTo prov
      }
    }
  }
}
