package io.renku.cli.model

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.ActivityGenerators
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class CliActivitySpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with DiffShouldMatcher {

  implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.sample.get

  val activityGen = ActivityGenerators.activityGen(Instant.now)

  "decode/encode" should {
    "be compatible" in {
      forAll(activityGen) { cliActivity =>
        val jsonLD = cliActivity.asJsonLD
        val back = jsonLD.cursor
          .as[CliActivity]
          .fold(throw _, identity)

        back shouldMatchTo cliActivity
      }
    }
  }
}
