package io.renku.cli.model

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.CommandParameterGenerators
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliParameterLinkSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with DiffShouldMatcher {

  val parameterLinkGen = CommandParameterGenerators.parameterLinkGen

  "decode/encode" should {
    "be compatible" in {
      forAll(parameterLinkGen) { cliParam =>
        val jsonLD = cliParam.asJsonLD
        val back = jsonLD.cursor
          .as[CliParameterLink]
          .fold(throw _, identity)

        back shouldMatchTo cliParam
      }
    }
  }
}
