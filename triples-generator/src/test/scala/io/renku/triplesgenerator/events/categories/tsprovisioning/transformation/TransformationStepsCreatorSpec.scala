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

package io.renku.triplesgenerator.events.categories.tsprovisioning.transformation

import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.triplesgenerator.events.categories.tsprovisioning.transformation.Generators._
import io.renku.triplesgenerator.events.categories.tsprovisioning.transformation.activities.ActivityTransformer
import io.renku.triplesgenerator.events.categories.tsprovisioning.transformation.datasets.DatasetTransformer
import io.renku.triplesgenerator.events.categories.tsprovisioning.transformation.persondetails.PersonTransformer
import io.renku.triplesgenerator.events.categories.tsprovisioning.transformation.projects.ProjectTransformer
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class TransformationStepsCreatorSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "createSteps" should {
    "combine steps from person/dataset/project transformers" in {
      val steps @ step1 :: step2 :: step3 :: step4 :: Nil = transformationSteps[Try].generateFixedSizeList(4)

      val personTransformer = mock[PersonTransformer[Try]]
      (() => personTransformer.createTransformationStep).expects().returning(step1)

      val projectTransformer = mock[ProjectTransformer[Try]]
      (() => projectTransformer.createTransformationStep).expects().returning(step2)

      val datasetTransformer = mock[DatasetTransformer[Try]]
      (() => datasetTransformer.createTransformationStep).expects().returning(step3)

      val activityTransformer = mock[ActivityTransformer[Try]]
      (() => activityTransformer.createTransformationStep).expects().returning(step4)

      new TransformationStepsCreatorImpl[Try](personTransformer,
                                              projectTransformer,
                                              datasetTransformer,
                                              activityTransformer
      ).createSteps shouldBe steps
    }
  }
}