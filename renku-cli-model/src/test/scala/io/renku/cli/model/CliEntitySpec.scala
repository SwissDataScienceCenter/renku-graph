package io.renku.cli.model

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.EntityGenerators
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliEntitySpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with DiffShouldMatcher {

  implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.sample.get

  val entityGen = EntityGenerators.entityGen()

  "decode/encode" should {
    "be compatible" in {
      forAll(entityGen) { cliEntity =>
        val jsonLD = cliEntity.asJsonLD
        val back = jsonLD.cursor
          .as[CliEntity]
          .fold(throw _, identity)

        back shouldMatchTo cliEntity
      }
    }
  }
}
