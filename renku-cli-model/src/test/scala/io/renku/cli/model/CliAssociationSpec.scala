package io.renku.cli.model

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.AssociationGenerators
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class CliAssociationSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with DiffShouldMatcher {

  implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.sample.get

  val associationGen = AssociationGenerators.associationGen(Instant.now)

  "decode/encode" should {
    "be compatible" in {
      forAll(associationGen) { cliAssoc =>
        val jsonLD = cliAssoc.asJsonLD
        val back = jsonLD.cursor
          .as[CliAssociation]
          .fold(throw _, identity)

        back shouldMatchTo cliAssoc
      }
    }
  }
}
