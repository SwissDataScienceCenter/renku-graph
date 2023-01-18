package io.renku.cli.model

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.DatasetFileGenerators
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class CliDatasetFileSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with DiffShouldMatcher {

  implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.sample.get

  val datasetFileGen = DatasetFileGenerators.datasetFileGen(Instant.now)

  "decode/encode" should {
    "be compatible" in {
      forAll(datasetFileGen) { cliDatasetFile =>
        val jsonLD = cliDatasetFile.asJsonLD
        val back = jsonLD.cursor
          .as[CliDatasetFile]
          .fold(throw _, identity)

        back shouldMatchTo cliDatasetFile
      }
    }
  }
}
