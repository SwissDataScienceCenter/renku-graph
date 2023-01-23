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
import io.renku.cli.model.generators.CommandParameterGenerators
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliParameterMappingSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with JsonLDCodecMatchers {

  val parameterMappingGen = CommandParameterGenerators.parameterMappingGen

  "decode/encode" should {
    "be compatible" in {
      forAll(parameterMappingGen) { cliParam =>
        assertCompatibleCodec(cliParam)
      }
    }

    "work on multiple items" in {
      forAll(parameterMappingGen, parameterMappingGen) { (cliParam1, cliParam2) =>
        assertCompatibleCodec(cliParam1, cliParam2)
      }
    }
  }
}