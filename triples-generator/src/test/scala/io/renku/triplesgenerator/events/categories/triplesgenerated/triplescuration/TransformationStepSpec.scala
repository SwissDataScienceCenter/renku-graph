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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration

import cats.data.EitherT
import ch.datascience.generators.CommonGraphGenerators.sparqlQueries
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.testentities._
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.{ResultData, TransformationStepResult}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class TransformationStepSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {

    "executes step's transformation" in {
      val stepTransformation = mockFunction[entities.Project, TransformationStepResult[Try]]
      val step               = TransformationStep(nonBlankStrings().generateOne, stepTransformation)

      val project = projectEntitiesWithDatasetsAndActivities.generateOne.to[entities.Project]

      val result = EitherT.rightT[Try, ProcessingRecoverableError](
        ResultData(projectEntitiesWithDatasetsAndActivities.generateOne.to[entities.Project],
                   sparqlQueries.generateList()
        )
      )
      stepTransformation.expects(project).returning(result)

      step.run(project) shouldBe result
    }
  }
}
