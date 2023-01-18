package io.renku.cli.model

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.PersonGenerators
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliPersonSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with DiffShouldMatcher {

  implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.sample.get

  val personGen = PersonGenerators.cliPersonGen

  "decode/encode" should {
    "be compatible" in {
      forAll(personGen) { cliPerson =>
        val jsonLD = cliPerson.asJsonLD
        val back = jsonLD.cursor
          .as[CliPerson]
          .fold(throw _, identity)

        back shouldMatchTo cliPerson
      }
    }
  }
}
