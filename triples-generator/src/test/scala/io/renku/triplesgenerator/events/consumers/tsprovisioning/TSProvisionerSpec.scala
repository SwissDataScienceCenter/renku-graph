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

package io.renku.triplesgenerator.events.consumers.tsprovisioning

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.entities
import io.renku.graph.model.entities.Project
import io.renku.graph.model.testentities._
import io.renku.triplesgenerator.generators.ErrorGenerators.logWorthyRecoverableErrors
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import transformation.Generators.transformationSteps
import transformation.TransformationStepsCreator
import triplesuploading.TriplesUploadResult.DeliverySuccess
import triplesuploading.{TransformationStepsRunner, TriplesUploadResult}

class TSProvisionerSpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "succeed if if creating the steps and running them succeeds" in {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    givenSuccessfulTriplesTransformationAndUpload(project)

    provisioner.provisionTS(project).asserting(_ shouldBe DeliverySuccess)
  }

  it should "return a RecoverableFailure if one occurs during steps running" in {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    val failure = TriplesUploadResult.RecoverableFailure(logWorthyRecoverableErrors.generateOne)
    givenSuccessfulStepsCreation(project, runningToReturn = failure.pure[IO])

    provisioner.provisionTS(project).asserting(_ shouldBe failure)
  }

  it should "return a NonRecoverableFailure if one occurs during steps running" in {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    val failure = TriplesUploadResult.NonRecoverableFailure(nonEmptyStrings().generateOne)
    givenSuccessfulStepsCreation(project, runningToReturn = failure.pure[IO])

    provisioner.provisionTS(project).asserting(_ shouldBe failure)
  }

  private val stepsCreator     = mock[TransformationStepsCreator[IO]]
  private val stepsRunner      = mock[TransformationStepsRunner[IO]]
  private lazy val provisioner = new TSProvisionerImpl[IO](stepsCreator, stepsRunner)

  private def givenSuccessfulTriplesTransformationAndUpload(project: Project) =
    givenSuccessfulStepsCreation(project, runningToReturn = DeliverySuccess.pure[IO])

  private def givenSuccessfulStepsCreation(project: Project, runningToReturn: IO[TriplesUploadResult]) = {
    val steps = transformationSteps[IO].generateList()
    (() => stepsCreator.createSteps)
      .expects()
      .returning(steps)

    givenStepsRunnerFor(steps, project, returning = runningToReturn)
  }

  private def givenStepsRunnerFor(steps:     List[TransformationStep[IO]],
                                  project:   Project,
                                  returning: IO[TriplesUploadResult]
  ) = (stepsRunner
    .run(_: List[TransformationStep[IO]], _: Project))
    .expects(steps, project)
    .returning(returning)
}
