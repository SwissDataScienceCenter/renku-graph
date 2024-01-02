/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.cli.model.generators

import io.circe.Json
import io.renku.cli.model.CliParameterValue
import io.renku.generators.Generators
import io.renku.graph.model.RenkuTinyTypeGenerators
import io.renku.jsonld.JsonLD
import org.scalacheck.Gen

trait ParameterValueGenerators {
  def parameterValueValueGen: Gen[CliParameterValue.Value] = {
    val bool = Gen.oneOf(true, false).map(JsonLD.fromBoolean)
    val num  = Gen.chooseNum(-3000.0, 3000.0).map(d => JsonLD.fromNumber(Json.fromDouble(d).flatMap(_.asNumber).get))
    val str  = Generators.nonEmptyStrings().map(JsonLD.fromString)
    val date = Generators.zonedDateTimes.map(_.toInstant).map(JsonLD.fromInstant)
    Gen
      .frequency(
        1 -> bool,
        2 -> num,
        1 -> date,
        2 -> str
      )
      .map(CliParameterValue.Value.apply)
  }

  def parameterValueGen: Gen[CliParameterValue] =
    for {
      id    <- RenkuTinyTypeGenerators.parameterValueIdGen
      ref   <- RenkuTinyTypeGenerators.commandParameterResourceId
      value <- parameterValueValueGen
    } yield CliParameterValue(id, ref, value)
}

object ParameterValueGenerators extends ParameterValueGenerators
