package io.renku.cli.model

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.PlanGenerators
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class CliPlanSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with DiffShouldMatcher {

  implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.sample.get
  val planGen                      = PlanGenerators.planGen(Instant.now)
  val compositePlanGen             = PlanGenerators.compositePlanGen(Instant.now)
  val workflowFilePlanGen          = PlanGenerators.workflowFilePlanGen(Instant.now)
  val workflowFileCompositePlanGen = PlanGenerators.workflowFileCompositePlanGen(Instant.now)

  "plan decode/encode" should {
    "be compatible" in {
      forAll(planGen) { cliPlan =>
        val jsonLD = cliPlan.asJsonLD
        val back = jsonLD.cursor
          .as[CliPlan]
          .fold(throw _, identity)

        back shouldMatchTo cliPlan
      }
    }
  }

  "composite plan decode/encode" should {
    "be compatible" in {
      forAll(compositePlanGen) { cliPlan =>
        val jsonLD = cliPlan.asJsonLD
        val back = jsonLD.cursor
          .as[CliCompositePlan]
          .fold(throw _, identity)

        back shouldMatchTo cliPlan
      }
    }
  }

  "workflow file plan decode/encode" should {
    "be compatible" in {
      forAll(workflowFilePlanGen) { cliPlan =>
        val jsonLD = cliPlan.asJsonLD
        val back = jsonLD.cursor
          .as[CliWorkflowFilePlan]
          .fold(throw _, identity)

        back shouldMatchTo cliPlan
      }
    }
  }

  "workflow file composite plan decode/encode" should {
    "be compatible" in {
      forAll(workflowFileCompositePlanGen) { cliPlan =>
        val jsonLD = cliPlan.asJsonLD
        val back = jsonLD.cursor
          .as[CliWorkflowFileCompositePlan]
          .fold(throw _, identity)

        back shouldMatchTo cliPlan
      }
    }
  }
}
