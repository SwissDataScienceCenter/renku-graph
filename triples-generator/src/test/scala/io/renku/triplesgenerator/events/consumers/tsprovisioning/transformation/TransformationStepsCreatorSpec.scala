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

package io.renku.triplesgenerator.events.consumers
package tsprovisioning.transformation

import Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.TSVersion
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class TransformationStepsCreatorSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "createSteps[DefaultGraph]" should {
    "combine steps from person/project/dataset/activity transformers for the DefaultGraph" in new TestCase {
      val steps @ step1 :: step2 :: step3 :: step4 :: Nil = transformationSteps[Try].generateFixedSizeList(4)

      (() => dgPersonTransformer.createTransformationStep).expects().returning(step1)
      (() => dgProjectTransformer.createTransformationStep).expects().returning(step2)
      (() => dgDatasetTransformer.createTransformationStep).expects().returning(step3)
      (() => dgActivityTransformer.createTransformationStep).expects().returning(step4)

      stepsCreator.createSteps(TSVersion.DefaultGraph) shouldBe steps
    }
  }

  "createSteps[NamedGraphs]" should {
    "combine steps from person/project/dataset/activity transformers for the NamedGraphs" in new TestCase {
      val steps @ step1 :: step2 :: step3 :: step4 :: Nil = transformationSteps[Try].generateFixedSizeList(4)

      (() => ngPersonTransformer.createTransformationStep).expects().returning(step1)
      (() => ngProjectTransformer.createTransformationStep).expects().returning(step2)
      (() => ngDatasetTransformer.createTransformationStep).expects().returning(step3)
      (() => ngActivityTransformer.createTransformationStep).expects().returning(step4)

      stepsCreator.createSteps(TSVersion.NamedGraphs) shouldBe steps
    }
  }

  private trait TestCase {
    val dgPersonTransformer   = mock[defaultgraph.persons.PersonTransformer[Try]]
    val dgProjectTransformer  = mock[defaultgraph.projects.ProjectTransformer[Try]]
    val dgDatasetTransformer  = mock[defaultgraph.datasets.DatasetTransformer[Try]]
    val dgActivityTransformer = mock[defaultgraph.activities.ActivityTransformer[Try]]
    val ngPersonTransformer   = mock[namedgraphs.persons.PersonTransformer[Try]]
    val ngProjectTransformer  = mock[namedgraphs.projects.ProjectTransformer[Try]]
    val ngDatasetTransformer  = mock[namedgraphs.datasets.DatasetTransformer[Try]]
    val ngActivityTransformer = mock[namedgraphs.activities.ActivityTransformer[Try]]
    val stepsCreator = new TransformationStepsCreatorImpl[Try](
      dgPersonTransformer,
      dgProjectTransformer,
      dgDatasetTransformer,
      dgActivityTransformer,
      ngPersonTransformer,
      ngProjectTransformer,
      ngDatasetTransformer,
      ngActivityTransformer
    )
  }
}
