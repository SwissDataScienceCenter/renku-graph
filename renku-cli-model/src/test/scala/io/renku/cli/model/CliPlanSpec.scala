/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.cli.model

import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.PlanGenerators
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class CliPlanSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with JsonLDCodecMatchers {

  implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.sample.get
  val planGen                      = PlanGenerators.planGen(Instant.now)
  val compositePlanGen             = PlanGenerators.compositePlanGen(Instant.now)
  val workflowFilePlanGen          = PlanGenerators.workflowFilePlanGen(Instant.now)
  val workflowFileCompositePlanGen = PlanGenerators.workflowFileCompositePlanGen(Instant.now)

  "plan decode/encode" should {
    "be compatible" in {
      forAll(planGen) { cliPlan =>
        assertCompatibleCodec(cliPlan)
      }
    }

    "work on multiple items" in {
      forAll(planGen, planGen) { (cliPan1, cliPlan2) =>
        assertCompatibleCodec(cliPan1, cliPlan2)
      }
    }
  }

  def allCompositePlans(in: CliCompositePlan): List[CliCompositePlan] =
    in :: in.plans.collect { case CliCompositePlan.ChildPlan.Composite(p) => p }.flatMap(allCompositePlans)

  "composite plan decode/encode" should {
    "be compatible" in {
      forAll(compositePlanGen) { cliPlan =>
        assertCompatibleCodec(allCompositePlans _)(cliPlan)
      }
    }
    "work on multiple items" in {
      forAll(compositePlanGen, compositePlanGen) { (cliPan1, cliPlan2) =>
        assertCompatibleCodec(allCompositePlans _)(cliPan1, cliPlan2)
      }
    }
  }

  "workflow file plan decode/encode" should {
    "be compatible" in {
      forAll(workflowFilePlanGen) { cliPlan =>
        assertCompatibleCodec(cliPlan)
      }
    }

    "work on multiple items" in {
      forAll(workflowFilePlanGen, workflowFilePlanGen) { (cliPan1, cliPlan2) =>
        assertCompatibleCodec(cliPan1, cliPlan2)
      }
    }
  }

  "workflow file composite plan decode/encode" should {
    "be compatible" in {
      forAll(workflowFileCompositePlanGen) { cliPlan =>
        assertCompatibleCodec(cliPlan)
      }
    }
    "work on multiple items" in {
      forAll(workflowFileCompositePlanGen, workflowFileCompositePlanGen) { (cliPan1, cliPlan2) =>
        assertCompatibleCodec(cliPan1, cliPlan2)
      }
    }
  }
}