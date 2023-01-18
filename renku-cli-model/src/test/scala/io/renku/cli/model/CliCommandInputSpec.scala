package io.renku.cli.model

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.CommandParameterGenerators
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliCommandInputSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with DiffShouldMatcher {

  val commandInputGen = CommandParameterGenerators.commandInputGen

  "decode/encode" should {
    "be compatible" in {
      forAll(commandInputGen) { cliParam =>
        val jsonLD = cliParam.asJsonLD
        val back = jsonLD.cursor
          .as[CliCommandInput]
          .fold(throw _, identity)

        back shouldMatchTo cliParam
      }
    }
  }
}
