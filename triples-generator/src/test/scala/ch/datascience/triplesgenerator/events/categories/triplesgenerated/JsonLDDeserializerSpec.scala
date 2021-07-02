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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import cats.effect.{ContextShift, IO}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.projects.ForksCount
import ch.datascience.graph.model.testentities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import ch.datascience.graph.model.testentities._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedGenerators._
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.global

class JsonLDDeserializerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "deserializeToModel" should {

    "successfully deserialize JsonLD to the model" in new TestCase {
      val paramValue = parameterDefaultValues.generateOne
      val input      = entityLocations.generateOne
      val output     = entityLocations.generateOne
      val activity = executionPlanners(
        runPlanEntities(
          CommandParameter.from(paramValue),
          CommandInput.fromLocation(input),
          CommandOutput.fromLocation(output)
        ),
        fixed(project)
      ).generateOne
        .planParameterValues(paramValue -> parameterValueOverrides.generateOne)
        .planInputParameterValuesFromChecksum(input -> entityChecksums.generateOne)
        .buildProvenanceGraph
        .fold(errors => fail(errors.toList.mkString), identity)

      val dataset = datasetEntities(ofAnyProvenance).generateOne

      val Right(metadata) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(triples = JsonLD.arr(activity.asJsonLD, dataset.asJsonLD))
        )
        .value
        .unsafeRunSync()

      metadata.activities.map(_.asJsonLD) should contain theSameElementsAs List(activity.asJsonLD)
      metadata.datasets.map(_.asJsonLD)   should contain theSameElementsAs List(dataset.asJsonLD)
    }

    "fail if fetching the project info fails" in new TestCase {}

    "fail if fetching users and members fails" in new TestCase {}

    "fail if the deserialization fails" in new TestCase {
      val triplesGeneratedEvent = triplesGeneratedEvents.generateOne
      deserializer.deserializeToModel(triplesGeneratedEvent).value.unsafeRunSync() shouldBe triplesGeneratedEvent
    }

//    val stdInInput  = entityLocations.generateOne
//    val stdOutInput = entityLocations.generateOne
//    val stdErrInput = entityLocations.generateOne
//    val activity2 = executionPlanners(
//      runPlanEntities(
//        CommandInput.streamedFromLocation(stdInInput),
//        CommandOutput.streamedFromLocation(stdOutInput, CommandOutput.stdOut),
//        CommandOutput.streamedFromLocation(stdErrInput, CommandOutput.stdErr)
//      ),
//      fixed(project)
//    ).generateOne.buildProvenanceGraph
//      .fold(errors => fail(errors.toList.mkString), identity)

  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {
    val project = projectEntities[ForksCount](visibilityAny)(anyForksCount).generateOne

    val deserializer = new JsonLDDeserializerImpl[IO]()
  }
}
