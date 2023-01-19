package io.renku.cli.model

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.GenerationGenerators
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliGenerationSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with DiffShouldMatcher {

  val generationGen = GenerationGenerators.generationGen

  "decode/encode" should {
    "be compatible" in {
      forAll(generationGen) { cliGen =>
        val jsonLD = cliGen.asJsonLD
        val back = jsonLD.cursor
          .as[CliGeneration]
          .fold(throw _, identity)

        println(jsonLD.flatten.toOption.get.toJson.spaces2)
        println("---------------")
        back shouldMatchTo cliGen

      }
    }
  }
}
