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

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.PlanGenerators
import io.renku.cli.model.tools.JsonLDTools
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl, Schemas}
import io.renku.jsonld.JsonLDDecoder
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class CliStepPlanSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with EitherValues
    with DiffShouldMatcher
    with JsonLDCodecMatchers {

  private implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.generateOne
  private val planGen                      = PlanGenerators.stepPlanGen(Instant.EPOCH)
  private val compositePlanGen             = PlanGenerators.compositePlanGen(Instant.EPOCH)
  private val workflowFilePlanGen          = PlanGenerators.workflowFileStepPlanGen(Instant.EPOCH)
  private val workflowFileCompositePlanGen = PlanGenerators.workflowFileCompositePlanGen(Instant.EPOCH)

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

    "work with additional types" in {
      val planDecoder = JsonLDDecoder.decodeList(CliPlan.jsonLDDecoderLenientTyped)
      val plan        = PlanGenerators.compositePlanChildPlanGen(Instant.EPOCH).generateOne
      val newJson =
        JsonLDTools
          .view(plan)
          .selectByTypes(CliPlan.entityTypes)
          .addType(Schemas.renku / "SomeOtherType")
          .value

      val result = newJson.cursor.as[List[CliPlan]](planDecoder)
      result.value shouldMatchTo List(plan)
    }
  }

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

  def allCompositePlans(in: CliCompositePlan): List[CliCompositePlan] =
    in :: in.plans.collect { case CliPlan.Composite(p) => p }.flatMap(allCompositePlans)
}
