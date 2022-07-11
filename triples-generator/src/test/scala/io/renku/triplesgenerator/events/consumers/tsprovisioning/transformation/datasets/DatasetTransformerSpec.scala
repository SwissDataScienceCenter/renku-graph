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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation
package datasets

import Generators.{queriesGen, recoverableClientErrors}
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesgenerator.events.consumers.tsprovisioning.TransformationStep.Queries
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class DatasetTransformerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "createTransformationStep" should {

    "create a step that runs all defined transformations" in new TestCase {
      val step1Result = generateProjAndQueries
      (() => derivationHierarchyUpdater.fixDerivationHierarchies)
        .expects()
        .returning(transformation(in = initialProjectAndQueries, out = step1Result))

      val step2Result = generateProjAndQueries
      (() => sameAsUpdater.updateSameAs)
        .expects()
        .returning(transformation(in = step1Result, out = step2Result))

      val step3Result = generateProjAndQueries
      (() => topmostSameAsUpdater.updateTopmostSameAs)
        .expects()
        .returning(transformation(in = step2Result, out = step3Result))

      val step4Result = generateProjAndQueries
      (() => originalIdentifierUpdater.updateOriginalIdentifiers)
        .expects()
        .returning(transformation(in = step3Result, out = step4Result))

      val step5Result = generateProjAndQueries
      (() => dateCreatedUpdater.updateDateCreated)
        .expects()
        .returning(transformation(in = step4Result, out = step5Result))

      val step6Result = generateProjAndQueries
      (() => descriptionUpdater.updateDescriptions)
        .expects()
        .returning(transformation(in = step5Result, out = step6Result))

      val step7Result = generateProjAndQueries
      (() => personLinksUpdater.updatePersonLinks)
        .expects()
        .returning(transformation(in = step6Result, out = step7Result))

      val step8Result = generateProjAndQueries
      (() => hierarchyOnInvalidationUpdater.updateHierarchyOnInvalidation)
        .expects()
        .returning(transformation(in = step7Result, out = step8Result))

      val step = transformer.createTransformationStep
      step.name.value shouldBe "Dataset Details Updates"

      (step run initialProjectAndQueries._1).value shouldBe step8Result.asRight.pure[Try]
    }

    "return the ProcessingRecoverableFailure if one of the steps fails with a recoverable failure" in new TestCase {

      val step1Result = generateProjAndQueries
      (() => derivationHierarchyUpdater.fixDerivationHierarchies)
        .expects()
        .returning(transformation(in = initialProjectAndQueries, out = step1Result))

      val exception = recoverableClientErrors.generateOne
      (() => sameAsUpdater.updateSameAs)
        .expects()
        .returning(transformation(in = step1Result, out = exception.raiseError[Try, (entities.Project, Queries)]))

      (() => topmostSameAsUpdater.updateTopmostSameAs)
        .expects()
        .returning(transformation(in = generateProjAndQueries, out = generateProjAndQueries))

      (() => originalIdentifierUpdater.updateOriginalIdentifiers)
        .expects()
        .returning(transformation(in = generateProjAndQueries, out = generateProjAndQueries))

      (() => dateCreatedUpdater.updateDateCreated)
        .expects()
        .returning(transformation(in = generateProjAndQueries, out = generateProjAndQueries))

      (() => descriptionUpdater.updateDescriptions)
        .expects()
        .returning(transformation(in = generateProjAndQueries, out = generateProjAndQueries))

      (() => personLinksUpdater.updatePersonLinks)
        .expects()
        .returning(transformation(in = generateProjAndQueries, out = generateProjAndQueries))

      (() => hierarchyOnInvalidationUpdater.updateHierarchyOnInvalidation)
        .expects()
        .returning(transformation(in = generateProjAndQueries, out = generateProjAndQueries))

      val step = transformer.createTransformationStep

      val Success(Left(recoverableError)) = (step run initialProjectAndQueries._1).value

      recoverableError          shouldBe a[ProcessingRecoverableError]
      recoverableError.getMessage should startWith("Problem finding dataset details in KG")
    }

    "fail with NonRecoverableFailure if one of the steps fails with an unknown exception" in new TestCase {
      val step1Result = generateProjAndQueries
      (() => derivationHierarchyUpdater.fixDerivationHierarchies)
        .expects()
        .returning(transformation(in = initialProjectAndQueries, out = step1Result))

      val exception = exceptions.generateOne
      (() => sameAsUpdater.updateSameAs)
        .expects()
        .returning(transformation(in = step1Result, out = exception.raiseError[Try, (entities.Project, Queries)]))

      (() => topmostSameAsUpdater.updateTopmostSameAs)
        .expects()
        .returning(transformation(in = generateProjAndQueries, out = generateProjAndQueries))

      (() => originalIdentifierUpdater.updateOriginalIdentifiers)
        .expects()
        .returning(transformation(in = generateProjAndQueries, out = generateProjAndQueries))

      (() => dateCreatedUpdater.updateDateCreated)
        .expects()
        .returning(transformation(in = generateProjAndQueries, out = generateProjAndQueries))

      (() => descriptionUpdater.updateDescriptions)
        .expects()
        .returning(transformation(in = generateProjAndQueries, out = generateProjAndQueries))

      (() => personLinksUpdater.updatePersonLinks)
        .expects()
        .returning(transformation(in = generateProjAndQueries, out = generateProjAndQueries))

      (() => hierarchyOnInvalidationUpdater.updateHierarchyOnInvalidation)
        .expects()
        .returning(transformation(in = generateProjAndQueries, out = generateProjAndQueries))

      val step = transformer.createTransformationStep

      (step run initialProjectAndQueries._1).value shouldBe exception.raiseError[Try, (entities.Project, Queries)]
    }
  }

  private trait TestCase {
    val initialProjectAndQueries = projectEntities(anyVisibility).generateOne.to[entities.Project] -> Queries.empty

    def transformation(in:  (entities.Project, Queries),
                       out: (entities.Project, Queries)
    ): ((entities.Project, Queries)) => Try[(entities.Project, Queries)] =
      transformation(in, out.pure[Try])

    def transformation(in:  (entities.Project, Queries),
                       out: Try[(entities.Project, Queries)]
    ): ((entities.Project, Queries)) => Try[(entities.Project, Queries)] = {
      case `in` => out
      case _    => fail("Project or Queries different than expected")
    }

    val derivationHierarchyUpdater     = mock[DerivationHierarchyUpdater[Try]]
    val sameAsUpdater                  = mock[SameAsUpdater[Try]]
    val topmostSameAsUpdater           = mock[TopmostSameAsUpdater[Try]]
    val originalIdentifierUpdater      = mock[OriginalIdentifierUpdater[Try]]
    val dateCreatedUpdater             = mock[DateCreatedUpdater[Try]]
    val descriptionUpdater             = mock[DescriptionUpdater[Try]]
    val personLinksUpdater             = mock[PersonLinksUpdater[Try]]
    val hierarchyOnInvalidationUpdater = mock[HierarchyOnInvalidationUpdater[Try]]
    val transformer = new DatasetTransformerImpl[Try](derivationHierarchyUpdater,
                                                      sameAsUpdater,
                                                      topmostSameAsUpdater,
                                                      originalIdentifierUpdater,
                                                      dateCreatedUpdater,
                                                      descriptionUpdater,
                                                      personLinksUpdater,
                                                      hierarchyOnInvalidationUpdater
    )

    def generateProjAndQueries =
      projectEntities(anyVisibility).generateOne.to[entities.Project] -> queriesGen.generateOne
  }
}
