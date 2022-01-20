/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation

import cats.data.EitherT
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.{ProjectWithQueries, Queries}
import io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class TransformationStepSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {

    "executes step's transformation" in {
      val stepTransformation = mockFunction[entities.Project, ProjectWithQueries[Try]]
      val step               = TransformationStep(nonBlankStrings().generateOne, stepTransformation)

      val project = renkuProjectEntitiesWithDatasetsAndActivities.generateOne.to[entities.Project]

      val result = EitherT.rightT[Try, ProcessingRecoverableError](
        (renkuProjectEntitiesWithDatasetsAndActivities.generateOne.to[entities.Project], queriesGen.generateOne)
      )
      stepTransformation.expects(project).returning(result)

      step.run(project) shouldBe result
    }
  }
}

class QueriesSpec extends AnyWordSpec with should.Matchers {

  "combine" should {
    "put pre and post queries of one Queries object after the pre and post queries of other Queries" in {

      val queries0 = queriesGen.generateOne
      val queries1 = queriesGen.generateOne

      (queries0 |+| queries1) shouldBe Queries(queries0.preDataUploadQueries ::: queries1.preDataUploadQueries,
                                               queries0.postDataUploadQueries ::: queries1.postDataUploadQueries
      )
    }
  }
}
