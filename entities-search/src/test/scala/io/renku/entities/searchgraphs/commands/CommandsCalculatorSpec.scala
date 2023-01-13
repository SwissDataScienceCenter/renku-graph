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

package io.renku.entities.searchgraphs
package commands

import CalculatorInfoSetGenerators._
import Encoders._
import Generators._
import UpdateCommand._
import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.entities.searchgraphs.SearchInfoLens.linkVisibility
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.model.GraphModelGenerators.{projectResourceIds, projectVisibilities}
import io.renku.graph.model.projects
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.{MatchResult, Matcher, should}
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class CommandsCalculatorSpec extends AnyWordSpec with should.Matchers {

  private val calculateCommands = CommandsCalculator.calculateCommands[Try]

  "calculateCommands" should {

    "return no commands " +
      "when DS from model is associated in TS with the single project and has the same visibility" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val modelInfo = searchInfoObjectsGen(withLinkTo = someInfoSet.project.resourceId -> anyVisibility).generateOne

        val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo.some, maybeTSInfo = modelInfo.some)

        calculateCommands(infoSet) shouldBe Nil.pure[Try]
      }

    "return no commands " +
      "when DS from model is associated in TS with the model project along with other projects all having the same visibility" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val modelInfo = searchInfoObjectsGen(withLinkTo = someInfoSet.project.resourceId -> anyVisibility).generateOne

        val tsInfo = modelInfo.copy(links =
          modelInfo.links :::
            linkObjectsGen(modelInfo.topmostSameAs, fixed(modelInfo.visibility)).generateNonEmptyList()
        )

        val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo.some, maybeTSInfo = tsInfo.some)

        calculateCommands(infoSet) shouldBe Nil.pure[Try]
      }

    "return no commands " +
      "when DS from model is associated in TS with the model project along with other projects having narrower visibility" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val modelInfo = searchInfoObjectsGen(withLinkTo =
          someInfoSet.project.resourceId -> visibilities(minus = projects.Visibility.Private).generateOne
        ).generateOne

        val tsInfo = modelInfo.copy(
          links = modelInfo.links :+
            linkObjectsGen(modelInfo.topmostSameAs, visibilitiesNarrower(modelInfo.visibility)).generateOne
        )

        assume(modelInfo.visibility == tsInfo.visibility)
        tsInfo.links.forall(_.visibility <= modelInfo.visibility)

        val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo.some, maybeTSInfo = tsInfo.some)

        calculateCommands(infoSet) shouldBe Nil.pure[Try]
      }

    "create Inserts for the info from the model " +
      "when it does not exist the TS" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val modelInfo = searchInfoObjectsGen(withLinkTo = someInfoSet.project.resourceId -> anyVisibility).generateOne

        val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo.some, maybeTSInfo = None)

        calculateCommands(infoSet) should produce(modelInfo.asQuads(searchInfoEncoder).map(Insert).toList)
      }

    "return no commands " +
      "when DS from model is associated in TS with the model project along with other projects having broader visibility" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val modelInfo = searchInfoObjectsGen(withLinkTo =
          someInfoSet.project.resourceId -> visibilities(minus = projects.Visibility.Public).generateOne
        ).generateOne

        val tsInfo = modelInfo.copy(
          links = modelInfo.links :+
            linkObjectsGen(modelInfo.topmostSameAs, visibilitiesBroader(modelInfo.visibility)).generateOne
        )

        assume(modelInfo.visibility < tsInfo.visibility)
        tsInfo.links.forall(_.visibility >= modelInfo.visibility)

        val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo.some, maybeTSInfo = tsInfo.some)

        assume(modelInfo.visibility < tsInfo.visibility)

        calculateCommands(infoSet) shouldBe Nil.pure[Try]
      }

    "return Delete & Insert commands changing the info's visibility and adding the new Link " +
      "when DS from model is associated in TS only with other projects having narrower visibility" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val modelInfo = searchInfoObjectsGen(
          withLinkTo = someInfoSet.project.resourceId -> visibilities(minus = projects.Visibility.Private).generateOne
        ).generateOne

        val tsInfo = modelInfo.copy(links =
          linkObjectsGen(modelInfo.topmostSameAs, visibilitiesNarrower(modelInfo.visibility)).generateNonEmptyList()
        )

        assume(modelInfo.visibility >= tsInfo.visibility)

        val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo.some, maybeTSInfo = tsInfo.some)

        calculateCommands(infoSet) should produce(
          Insert(
            DatasetsQuad(tsInfo.topmostSameAs, SearchInfoOntology.visibilityProperty.id, modelInfo.visibility.asObject)
          ) ::
            Delete(
              DatasetsQuad(tsInfo.topmostSameAs, SearchInfoOntology.visibilityProperty.id, tsInfo.visibility.asObject)
            ) ::
            modelInfo.links.toList.flatMap(l => (l.asQuads + linkEdge(modelInfo, l)).map(Insert))
        )
      }

    "return Insert commands adding the new link " +
      "when DS from model is associated in TS only with other projects having broader or the same visibility" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val modelInfo = searchInfoObjectsGen(
          withLinkTo = someInfoSet.project.resourceId -> visibilities(minus = projects.Visibility.Public).generateOne
        ).generateOne

        val tsInfo = modelInfo.copy(links =
          linkObjectsGen(modelInfo.topmostSameAs, visibilitiesBroader(modelInfo.visibility)).generateNonEmptyList()
        )

        assume(modelInfo.visibility <= tsInfo.visibility)

        val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo.some, maybeTSInfo = tsInfo.some)

        calculateCommands(infoSet) should produce(
          modelInfo.links.toList.flatMap(l => (l.asQuads + linkEdge(modelInfo, l)).map(Insert))
        )
      }

    "return Delete & Insert commands updating visibility on the project link only " +
      "when DS from model is already associated in TS but its visibility became narrower although not changing TS info's visibility" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val modelInfo = searchInfoObjectsGen(
          withLinkTo = someInfoSet.project.resourceId -> projects.Visibility.Private
        ).generateOne
        val modelInfoLink = modelInfo.links.head

        val tsInfoLink = linkVisibility.set(projects.Visibility.Public)(modelInfoLink)
        val tsInfo = modelInfo.copy(links =
          NonEmptyList.of(tsInfoLink, linkObjectsGen(modelInfo.topmostSameAs, projects.Visibility.Public).generateOne)
        )

        assume(modelInfo.visibility < tsInfo.visibility)

        val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo.some, maybeTSInfo = tsInfo.some)

        calculateCommands(infoSet) should produce(
          Insert(
            DatasetsQuad(modelInfoLink.resourceId, LinkOntology.visibilityProperty.id, modelInfo.visibility.asObject)
          ) ::
            Delete(
              DatasetsQuad(tsInfoLink.resourceId, LinkOntology.visibilityProperty.id, tsInfoLink.visibility.asObject)
            ) :: Nil
        )
      }

    "return Delete & Insert commands updating visibility on the project link and info " +
      "when DS from model is already associated in TS but its visibility became narrower causing changing TS info's visibility" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val modelInfo = searchInfoObjectsGen(
          withLinkTo = someInfoSet.project.resourceId -> projects.Visibility.Private
        ).generateOne
        val modelInfoLink = modelInfo.links.head

        val tsInfoLink = linkVisibility.set(visibilitiesBroader(modelInfoLink.visibility).generateOne)(modelInfoLink)
        val tsInfo     = modelInfo.copy(links = NonEmptyList.of(tsInfoLink))

        assume(modelInfo.visibility < tsInfo.visibility)

        val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo.some, maybeTSInfo = tsInfo.some)

        calculateCommands(infoSet) should produce(
          Insert(
            DatasetsQuad(modelInfoLink.resourceId, LinkOntology.visibilityProperty.id, modelInfo.visibility.asObject)
          ) ::
            Delete(
              DatasetsQuad(tsInfoLink.resourceId, LinkOntology.visibilityProperty.id, tsInfoLink.visibility.asObject)
            ) ::
            Insert(
              DatasetsQuad(tsInfo.topmostSameAs,
                           SearchInfoOntology.visibilityProperty.id,
                           modelInfo.visibility.asObject
              )
            ) ::
            Delete(
              DatasetsQuad(tsInfo.topmostSameAs,
                           SearchInfoOntology.visibilityProperty.id,
                           tsInfoLink.visibility.asObject
              )
            ) :: Nil
        )
      }

    "return Delete & Insert commands updating visibility on the info and on the project link " +
      "when DS from model is already associated in TS but its visibility became broader than other projects" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val modelInfo = searchInfoObjectsGen(
          withLinkTo = someInfoSet.project.resourceId -> projects.Visibility.Public
        ).generateOne
        val modelInfoLink = modelInfo.links.head

        val tsInfoLink = linkVisibility.set(projects.Visibility.Private)(modelInfoLink)
        val tsInfo = modelInfo.copy(links =
          NonEmptyList.of(tsInfoLink,
                          linkObjectsGen(modelInfo.topmostSameAs,
                                         visibilitiesNarrower(modelInfo.visibility)
                          ).generateOne
          )
        )

        assume(modelInfo.visibility > tsInfo.visibility)

        val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo.some, maybeTSInfo = tsInfo.some)

        calculateCommands(infoSet) should produce(
          Insert(
            DatasetsQuad(modelInfo.topmostSameAs,
                         SearchInfoOntology.visibilityProperty.id,
                         modelInfo.visibility.asObject
            )
          ) ::
            Delete(
              DatasetsQuad(tsInfo.topmostSameAs, SearchInfoOntology.visibilityProperty.id, tsInfo.visibility.asObject)
            ) ::
            Insert(
              DatasetsQuad(modelInfoLink.resourceId, LinkOntology.visibilityProperty.id, modelInfo.visibility.asObject)
            ) ::
            Delete(
              DatasetsQuad(tsInfoLink.resourceId, LinkOntology.visibilityProperty.id, tsInfoLink.visibility.asObject)
            ) :: Nil
        )
      }

    "create Deletes for the info with association to the project only found in the TS " +
      "when it does not exist on the Project" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val tsInfo = searchInfoObjectsGen(withLinkTo = someInfoSet.project.resourceId -> anyVisibility).generateOne

        val infoSet = someInfoSet.copy(maybeModelInfo = None, maybeTSInfo = tsInfo.some)

        calculateCommands(infoSet) should produce(tsInfo.asQuads.map(Delete).toList)
      }

    "create Deletes for the project link found on the info in TS " +
      "when the info found in the TS is associated with other projects - other projects' visibilities are broader" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val tsInfo = searchInfoObjectsGen(
          withLinkTo = someInfoSet.project.resourceId -> projects.Visibility.Private,
          and = projectResourceIds.generateOne        -> visibilitiesBroader(projects.Visibility.Private).generateOne
        ).generateOne

        val infoSet = someInfoSet.copy(maybeModelInfo = None, maybeTSInfo = tsInfo.some)

        val expectedLinkToDelete = tsInfo.links
          .find(_.projectId == someInfoSet.project.resourceId)
          .getOrElse(fail("There supposed to be a link to delete"))

        calculateCommands(infoSet) should produce(
          (expectedLinkToDelete.asQuads +
            DatasetsQuad(tsInfo.topmostSameAs,
                         SearchInfoOntology.linkProperty,
                         expectedLinkToDelete.resourceId.asEntityId
            )).map(Delete).toList
        )
      }

    "create Deletes for the project link found on the info in TS and Delete & Insert for the info visibility " +
      "when the info found in the TS is associated with other projects - other projects' visibilities are narrower" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val tsInfo = searchInfoObjectsGen(
          withLinkTo = someInfoSet.project.resourceId -> projects.Visibility.Public,
          and = projectResourceIds.generateOne        -> visibilitiesNarrower(projects.Visibility.Public).generateOne
        ).generateOne

        val infoSet = someInfoSet.copy(maybeModelInfo = None, maybeTSInfo = tsInfo.some)

        val expectedLinkToDelete = tsInfo.links
          .find(_.projectId == someInfoSet.project.resourceId)
          .getOrElse(fail("There supposed to be a link to delete"))
        val newVisibility =
          tsInfo.links.filterNot(_.projectId == someInfoSet.project.resourceId).map(_.visibility).max

        calculateCommands(infoSet) should produce(
          (expectedLinkToDelete.asQuads +
            DatasetsQuad(tsInfo.topmostSameAs,
                         SearchInfoOntology.linkProperty,
                         expectedLinkToDelete.resourceId.asEntityId
            )).map(Delete).toList :+
            Insert(
              DatasetsQuad(tsInfo.topmostSameAs, SearchInfoOntology.visibilityProperty.id, newVisibility.asObject)
            ) :+
            Delete(
              DatasetsQuad(tsInfo.topmostSameAs, SearchInfoOntology.visibilityProperty.id, tsInfo.visibility.asObject)
            )
        )
      }

    "fail for when input data in illegal state" in {

      val infoSet = calculatorInfoSets.generateOne.copy(maybeModelInfo = None, maybeTSInfo = None)

      val Failure(exception) = calculateCommands(infoSet).map(_.toSet)

      exception.getMessage shouldBe show"Cannot calculate update commands for $infoSet"
    }
  }

  private def visibilities(minus: projects.Visibility): Gen[projects.Visibility] =
    Gen.oneOf(projects.Visibility.all - minus)

  private def anyVisibility: projects.Visibility =
    projectVisibilities.generateOne

  private def visibilitiesNarrower(than: projects.Visibility): Gen[projects.Visibility] =
    Gen.oneOf(projects.Visibility.allOrdered.reverse.takeWhile(_ != than))

  private def visibilitiesBroader(than: projects.Visibility): Gen[projects.Visibility] =
    Gen.oneOf(projects.Visibility.allOrdered.takeWhile(_ != than))

  private def linkEdge(info: SearchInfo, link: Link): Quad =
    DatasetsQuad(info.topmostSameAs, SearchInfoOntology.linkProperty, link.resourceId.asEntityId)

  private def produce(expected: List[UpdateCommand]) = new Matcher[Try[List[UpdateCommand]]] {
    def apply(actual: Try[List[UpdateCommand]]): MatchResult = {
      val matching = actual.map(_.toSet == expected.toSet).fold(fail(_), identity)
      MatchResult(matching, s"\nExpected: $expected\nActual: $actual", s"\nExpected: $expected\nActual: $actual")
    }
  }
}
