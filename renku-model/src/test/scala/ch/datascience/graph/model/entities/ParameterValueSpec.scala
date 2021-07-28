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

package ch.datascience.graph.model.entities

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonEmptyStrings
import ch.datascience.graph.model.commandParameters.ParameterDefaultValue
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.testentities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import ch.datascience.graph.model.testentities._
import io.circe.DecodingFailure
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
          val activity = executionPlanners(
            planEntities(CommandParameter.from(defaultValue))
          ).generateOne
            .planParameterValues(defaultValue -> valueOverride)
            .buildProvenanceUnsafe()

          val Right(parameterValues) = activity.asJsonLD.flatten
            .fold(throw _, identity)
            .cursor
            .as[List[entities.ParameterValue]](
              decodeList(entities.ParameterValue.decoder(activity.plan))
            )

          parameterValues           shouldBe activity.parameters.map(_.to[entities.ParameterValue])
          parameterValues.foreach(_ shouldBe a[entities.ParameterValue.VariableParameterValue])
      }
    }

    "turn JsonLD InputParameterValue entity into the InputParameterValue object " in {
      forAll(entityLocations, entityChecksums) { (location, checksum) =>
        val activity = executionPlanners(
          planEntities(CommandInput.fromLocation(location))
        ).generateOne
          .planInputParameterValuesFromChecksum(location -> checksum)
          .buildProvenanceUnsafe()

        val Right(parameterValues) = activity.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.ParameterValue]](
            decodeList(entities.ParameterValue.decoder(activity.plan))
          )

        parameterValues           shouldBe activity.parameters.map(_.to[entities.ParameterValue])
        parameterValues.foreach(_ shouldBe a[entities.ParameterValue.InputParameterValue])
      }
    }

    "turn JsonLD OutputParameterValue entity into the OutputParameterValue object " in {
      forAll(entityLocations) { location =>
        val activity = executionPlanners(
          planEntities(CommandOutput.fromLocation(location))
        ).generateOne
          .buildProvenanceUnsafe()

        val Right(parameterValues) = activity.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.ParameterValue]](
            decodeList(entities.ParameterValue.decoder(activity.plan))
          )

        parameterValues           shouldBe activity.parameters.map(_.to[entities.ParameterValue])
        parameterValues.foreach(_ shouldBe a[entities.ParameterValue.OutputParameterValue])
      }
    }

    "fail if there are VariableParameterValue for non-existing CommandParameters" in {
      val defaultValue  = nonEmptyStrings().toGeneratorOf(ParameterDefaultValue).generateOne
      val valueOverride = parameterValueOverrides.generateOne
      val activity = executionPlanners(
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
      failure.message shouldBe s"VariableParameterValue points to a non-existing command parameter ${activity.plan.to[entities.Plan].parameters.map(_.resourceId).head}"
    }

    "fail if there are InputParameterValue for non-existing InputParameters" in {
      val location = entityLocations.generateOne
      val checksum = entityChecksums.generateOne
      val activity = executionPlanners(
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
      failure.message shouldBe s"PathParameterValue points to a non-existing command parameter ${activity.plan.to[entities.Plan].inputs.map(_.resourceId).head}"
    }

    "fail if there are OutputParameterValue for non-existing OutputParameters" in {
      val location = entityLocations.generateOne
      val activity = executionPlanners(
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
      failure.message shouldBe s"PathParameterValue points to a non-existing command parameter ${activity.plan.to[entities.Plan].outputs.map(_.resourceId).head}"
    }
  }
}
