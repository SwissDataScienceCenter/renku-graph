/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.entities

import io.circe.DecodingFailure
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.commandParameters.ParameterDefaultValue
import io.renku.graph.model.entities
import io.renku.graph.model.testentities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import io.renku.graph.model.testentities._
import io.renku.jsonld.JsonLDDecoder._
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ParameterValueSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "decode" should {

    "turn JsonLD VariableParameterValue entity into the VariableParameterValue object " in {
      forAll(nonEmptyStrings().toGeneratorOf(ParameterDefaultValue), parameterValueOverrides) {
        (defaultValue, valueOverride) =>
          val activity =
            executionPlannersDecoupledFromProject(planEntities(CommandParameter.from(defaultValue))).generateOne
              .planParameterValues(defaultValue -> valueOverride)
              .buildProvenanceUnsafe()
              .to[entities.Activity]

          val Right(parameterValues) = activity.asJsonLD.flatten
            .fold(throw _, identity)
            .cursor
            .as[List[entities.ParameterValue]](
              decodeList(entities.ParameterValue.decoder(activity.association.plan))
            )

          parameterValues           shouldBe activity.parameters
          parameterValues.foreach(_ shouldBe a[entities.ParameterValue.CommandParameterValue])
      }
    }

    "turn JsonLD InputParameterValue entity into the InputParameterValue object " in {
      forAll(entityLocations, entityChecksums) { (location, checksum) =>
        val activity = executionPlannersDecoupledFromProject(
          planEntities(CommandInput.fromLocation(location))
        ).generateOne
          .planInputParameterValuesFromChecksum(location -> checksum)
          .buildProvenanceUnsafe()
          .to[entities.Activity]

        val Right(parameterValues) = activity.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.ParameterValue]](
            decodeList(entities.ParameterValue.decoder(activity.association.plan))
          )

        parameterValues           shouldBe activity.parameters
        parameterValues.foreach(_ shouldBe a[entities.ParameterValue.CommandInputValue])
      }
    }

    "turn JsonLD OutputParameterValue entity into the OutputParameterValue object " in {
      forAll(entityLocations) { location =>
        val activity = executionPlannersDecoupledFromProject(
          planEntities(CommandOutput.fromLocation(location))
        ).generateOne
          .buildProvenanceUnsafe()
          .to[entities.Activity]

        val Right(parameterValues) = activity.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.ParameterValue]](
            decodeList(entities.ParameterValue.decoder(activity.association.plan))
          )

        parameterValues           shouldBe activity.parameters
        parameterValues.foreach(_ shouldBe a[entities.ParameterValue.CommandOutputValue])
      }
    }

    "fail if there are VariableParameterValue for non-existing CommandParameters" in {
      val defaultValue  = nonEmptyStrings().toGeneratorOf(ParameterDefaultValue).generateOne
      val valueOverride = parameterValueOverrides.generateOne
      val activity = executionPlannersDecoupledFromProject(
        planEntities(CommandParameter.from(defaultValue))
      ).generateOne
        .planParameterValues(defaultValue -> valueOverride)
        .buildProvenanceUnsafe()

      val Left(failure) = activity.asJsonLD.flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.ParameterValue]](
          decodeList(entities.ParameterValue.decoder(activity.plan.to[entities.Plan].copy(parameters = Nil)))
        )

      failure         shouldBe a[DecodingFailure]
      failure.message shouldBe s"ParameterValue points to a non-existing command parameter ${activity.plan.to[entities.Plan].parameters.map(_.resourceId).head}"
    }

    "fail if there are InputParameterValue for non-existing InputParameters" in {
      val location = entityLocations.generateOne
      val checksum = entityChecksums.generateOne
      val activity = executionPlannersDecoupledFromProject(
        planEntities(CommandInput.fromLocation(location))
      ).generateOne
        .planInputParameterValuesFromChecksum(location -> checksum)
        .buildProvenanceUnsafe()

      val Left(failure) = activity.asJsonLD.flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.ParameterValue]](
          decodeList(entities.ParameterValue.decoder(activity.plan.to[entities.Plan].copy(inputs = Nil)))
        )

      failure         shouldBe a[DecodingFailure]
      failure.message shouldBe s"ParameterValue points to a non-existing command parameter ${activity.plan.to[entities.Plan].inputs.map(_.resourceId).head}"
    }

    "fail if there are OutputParameterValue for non-existing OutputParameters" in {
      val location = entityLocations.generateOne
      val activity = executionPlannersDecoupledFromProject(
        planEntities(CommandOutput.fromLocation(location))
      ).generateOne
        .buildProvenanceUnsafe()

      val Left(failure) = activity.asJsonLD.flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.ParameterValue]](
          decodeList(entities.ParameterValue.decoder(activity.plan.to[entities.Plan].copy(outputs = Nil)))
        )

      failure         shouldBe a[DecodingFailure]
      failure.message shouldBe s"ParameterValue points to a non-existing command parameter ${activity.plan.to[entities.Plan].outputs.map(_.resourceId).head}"
    }
  }
}
