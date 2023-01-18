package io.renku.cli.model

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.ParameterValueGenerators
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliParameterValueSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with DiffShouldMatcher {

  val parameterValueGen = ParameterValueGenerators.parameterValueGen

  "decode/encode" should {
    "be compatible" in {
      forAll(parameterValueGen) { cliParameterValue =>
        val jsonLD = cliParameterValue.asJsonLD
        val back = jsonLD.cursor
          .as[CliParameterValue]
          .fold(throw _, identity)

        back shouldMatchTo cliParameterValue
      }
    }
  }
}
