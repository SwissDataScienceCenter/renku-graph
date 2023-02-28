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
import io.renku.cli.model.generators.GenerationGenerators
import io.renku.jsonld.JsonLDDecoder
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliGenerationSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with EitherValues
    with CliDiffInstances
    with DiffShouldMatcher
    with JsonLDCodecMatchers {

  val generationGen = GenerationGenerators.generationGen()

  "decode/encode" should {
    "be compatible" in {
      forAll(generationGen) { cliGen =>
        assertCompatibleCodec(cliGen)
      }
    }

    "work on multiple items" in {
      forAll(generationGen, generationGen) { (cliGen1, cliGen2) =>
        assertCompatibleCodec(cliGen1, cliGen2)
      }
    }

    "work for a specific activity only" in {
      forAll(generationGen, generationGen) { (gen1, gen2) =>
        val decoder = JsonLDDecoder.decodeList(CliGeneration.decoderForActivity(gen1.activityResourceId))
        val result  = List(gen1, gen2).asFlattenedJsonLD.cursor.as[List[CliGeneration]](decoder)
        result.value shouldMatchTo List(gen1)
      }
    }
  }
}
