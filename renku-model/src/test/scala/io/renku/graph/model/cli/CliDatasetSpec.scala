package io.renku.graph.model.cli

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.graph.model.{GitLabApiUrl, RenkuUrl, projects}
import io.renku.graph.model.entities.DiffInstances
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class CliDatasetSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with DiffInstances
    with DiffShouldMatcher {

  implicit val gitLabApiUrl: GitLabApiUrl = EntitiesGenerators.gitLabApiUrl
  implicit val renkuUrl:     RenkuUrl     = EntitiesGenerators.renkuUrl

  val datasetGen = EntitiesGenerators
    .datasetEntities(EntitiesGenerators.provenanceNonModified)
    .apply(projects.DateCreated(Instant.now))

  "decode/encode" should {
    "be compatible" in {
      forAll(datasetGen) { testDataset =>
        val cliDataset = testDataset.to[CliDataset]
        val jsonLD     = cliDataset.asJsonLD
        val back = jsonLD.cursor
          .as[CliDataset]
          .fold(throw _, identity)

        back shouldMatchTo cliDataset
      }
    }
  }
}
