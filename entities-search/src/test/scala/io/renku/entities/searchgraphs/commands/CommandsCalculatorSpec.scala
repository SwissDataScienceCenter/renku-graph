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

import Encoders._
import Generators._
import UpdateCommand._
import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.model.GraphModelGenerators.{projectResourceIds, projectVisibilities}
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.{MatchResult, Matcher, should}
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class CommandsCalculatorSpec extends AnyWordSpec with should.Matchers {

  private val calculateCommands = CommandsCalculator[Try]().calculateCommands

  "calculateCommands" should {

    "return no commands " +
      "when DS from the model and TS are the same" in {

        val project = newProject()

        val info = searchInfoObjectsGen(withLinkTo = project).generateOne

        val infoSet = CalculatorInfoSet.AllInfos(project,
                                                 info,
                                                 tsInfo = info,
                                                 tsVisibilities = Map(project.resourceId -> project.visibility)
        )

        calculateCommands(infoSet) shouldBe Nil.pure[Try]
      }

    "create Inserts for the info from the model " +
      "when it does not exist the TS" in {

        val project = newProject()

        val modelInfo = searchInfoObjectsGen(withLinkTo = project).generateOne

        val infoSet = CalculatorInfoSet.ModelInfoOnly(project, modelInfo)

        calculateCommands(infoSet) should produce(modelInfo.asQuads(searchInfoEncoder).map(Insert).toList)
      }

    "return no commands " +
      "when DS from model is associated in TS with the model project along with other projects all having the same visibility" in {

        val project = newProject()

        val modelInfo = searchInfoObjectsGen(withLinkTo = project).generateOne

        val tsInfo =
          modelInfo.copy(links = modelInfo.links ::: linkObjectsGen(modelInfo.topmostSameAs).generateNonEmptyList())

        val infoSet =
          CalculatorInfoSet.AllInfos(project,
                                     modelInfo,
                                     tsInfo,
                                     tsInfo.links.map(_.projectId).map(_ -> modelInfo.visibility).toList.toMap
          )

        calculateCommands(infoSet) shouldBe Nil.pure[Try]
      }

    "return no commands " +
      "when DS from model is associated in TS with the model project along with other projects having narrower visibility" in {

        val project = newProject(visibilities(minus = projects.Visibility.Private).generateOne)

        val modelInfo = searchInfoObjectsGen(withLinkTo = project).generateOne

        val otherTsLinks = linkObjectsGen(modelInfo.topmostSameAs).generateNonEmptyList()
        val tsInfo       = modelInfo.copy(links = modelInfo.links ::: otherTsLinks)
        val otherTsLinksVisibilities =
          otherTsLinks.map(_.projectId).map(_ -> visibilitiesNarrower(modelInfo.visibility).generateOne).toList.toMap

        val infoSet = CalculatorInfoSet.AllInfos(
          project,
          modelInfo,
          tsInfo,
          otherTsLinksVisibilities + (project.resourceId -> modelInfo.visibility)
        )

        calculateCommands(infoSet) shouldBe Nil.pure[Try]
      }

    "return no commands " +
      "when DS from model is associated in TS with the model project along with other projects having broader visibility" in {

        val project = newProject(visibilities(minus = projects.Visibility.Public).generateOne)

        val modelInfo = searchInfoObjectsGen(withLinkTo = project).generateOne

        val otherTsLinks = linkObjectsGen(modelInfo.topmostSameAs).generateNonEmptyList()
        val tsInfo       = modelInfo.copy(links = modelInfo.links ::: otherTsLinks)
        val otherTsLinksVisibilities =
          otherTsLinks.map(_.projectId).map(_ -> visibilitiesBroader(modelInfo.visibility).generateOne).toList.toMap

        val infoSet = CalculatorInfoSet.AllInfos(
          project,
          modelInfo,
          tsInfo,
          otherTsLinksVisibilities + (project.resourceId -> modelInfo.visibility)
        )

        calculateCommands(infoSet) shouldBe Nil.pure[Try]
      }

    "return Delete & Insert commands changing the info's visibility and adding the new Link " +
      "when DS from the model is associated in TS only with other projects having narrower visibility" in {

        val project = newProject(visibilities(minus = projects.Visibility.Private).generateOne)

        val modelInfo = searchInfoObjectsGen(withLinkTo = project).generateOne

        val tsInfoVisibility = visibilitiesNarrower(modelInfo.visibility).generateOne
        val tsInfoLink       = linkObjectsGen(modelInfo.topmostSameAs).generateOne
        val tsInfo           = modelInfo.copy(links = NonEmptyList.one(tsInfoLink), visibility = tsInfoVisibility)

        val infoSet =
          CalculatorInfoSet.AllInfos(project, modelInfo, tsInfo, Map(tsInfoLink.projectId -> tsInfoVisibility))

        calculateCommands(infoSet) should produce(
          Insert(visibilityQuad(tsInfo, modelInfo.visibility)) ::
            Delete(visibilityQuad(tsInfo, tsInfo.visibility)) ::
            modelInfo.links.toList.flatMap(l => (l.asQuads + linkEdge(modelInfo, l)).map(Insert))
        )
      }

    "return Insert commands adding the new link " +
      "when DS from model is associated in TS only with other projects having broader or the same visibility" in {

        val project = newProject(visibilities(minus = projects.Visibility.Public).generateOne)

        val modelInfo = searchInfoObjectsGen(withLinkTo = project).generateOne

        val tsInfoVisibility = visibilitiesBroader(modelInfo.visibility).generateOne
        val tsInfoLink1      = linkObjectsGen(modelInfo.topmostSameAs).generateOne
        val tsInfoLink2      = linkObjectsGen(modelInfo.topmostSameAs).generateOne
        val tsInfo = modelInfo.copy(links = NonEmptyList.of(tsInfoLink1, tsInfoLink2), visibility = tsInfoVisibility)

        val infoSet = CalculatorInfoSet.AllInfos(project,
                                                 modelInfo,
                                                 tsInfo,
                                                 Map(tsInfoLink1.projectId -> modelInfo.visibility,
                                                     tsInfoLink2.projectId -> tsInfoVisibility
                                                 )
        )

        calculateCommands(infoSet) should produce(
          modelInfo.links.toList.flatMap(l => (l.asQuads + linkEdge(modelInfo, l)).map(Insert))
        )
      }

    "return no commands " +
      "when DS from model is already associated in TS but its visibility became narrower although not changing TS info's visibility" in {

        val project = newProject(projects.Visibility.Private)

        val modelInfo = searchInfoObjectsGen(withLinkTo = project).generateOne

        val tsInfoVisibility = visibilitiesBroader(modelInfo.visibility).generateOne
        val otherTsInfoLink  = linkObjectsGen(modelInfo.topmostSameAs).generateOne
        val tsInfo           = modelInfo.copy(links = modelInfo.links :+ otherTsInfoLink, visibility = tsInfoVisibility)

        val infoSet = CalculatorInfoSet.AllInfos(project,
                                                 modelInfo,
                                                 tsInfo,
                                                 Map(project.resourceId        -> tsInfoVisibility,
                                                     otherTsInfoLink.projectId -> tsInfoVisibility
                                                 )
        )

        calculateCommands(infoSet) shouldBe Nil.pure[Try]
      }

    "return Delete & Insert commands changing the info's visibility " +
      "when DS from model is already associated in TS but its visibility became narrower causing TS info's visibility change" in {

        val project = newProject(projects.Visibility.Private)

        val modelInfo = searchInfoObjectsGen(withLinkTo = project).generateOne

        val tsInfoVisibility = visibilitiesBroader(modelInfo.visibility).generateOne
        val otherTsInfoLink  = linkObjectsGen(modelInfo.topmostSameAs).generateOne
        val tsInfo           = modelInfo.copy(links = modelInfo.links :+ otherTsInfoLink, visibility = tsInfoVisibility)

        val infoSet = CalculatorInfoSet.AllInfos(project,
                                                 modelInfo,
                                                 tsInfo,
                                                 Map(project.resourceId        -> tsInfoVisibility,
                                                     otherTsInfoLink.projectId -> modelInfo.visibility
                                                 )
        )

        calculateCommands(infoSet) should produce(
          Insert(visibilityQuad(tsInfo, modelInfo.visibility)) ::
            Delete(visibilityQuad(tsInfo, tsInfo.visibility)) :: Nil
        )
      }

    "return Delete & Insert commands changing the info's visibility " +
      "when DS from model is already associated in TS but its visibility became broader causing TS info's visibility change" in {

        val project = newProject(projects.Visibility.Public)

        val modelInfo = searchInfoObjectsGen(withLinkTo = project).generateOne

        val tsInfoVisibility = visibilitiesNarrower(modelInfo.visibility).generateOne
        val otherTsInfoLink  = linkObjectsGen(modelInfo.topmostSameAs).generateOne
        val tsInfo           = modelInfo.copy(links = modelInfo.links :+ otherTsInfoLink, visibility = tsInfoVisibility)

        val infoSet = CalculatorInfoSet.AllInfos(project,
                                                 modelInfo,
                                                 tsInfo,
                                                 Map(project.resourceId        -> tsInfoVisibility,
                                                     otherTsInfoLink.projectId -> tsInfoVisibility
                                                 )
        )

        calculateCommands(infoSet) should produce(
          Insert(visibilityQuad(tsInfo, modelInfo.visibility)) ::
            Delete(visibilityQuad(tsInfo, tsInfo.visibility)) :: Nil
        )
      }

    "create Deletes for the info and the link " +
      "when it does not exist on this Project and any other" in {

        val project = newProject()

        val tsInfo = searchInfoObjectsGen(withLinkTo = project).generateOne

        val infoSet = CalculatorInfoSet.TSInfoOnly(project, tsInfo, Map(project.resourceId -> tsInfo.visibility))

        calculateCommands(infoSet) should produce(tsInfo.asQuads.map(Delete).toList)
      }

    "create Deletes for the link only " +
      "when the info found in the TS is associated also with other projects - the other projects' visibilities are broader or same" in {

        val project = newProject()

        val otherTsProject1Id = projectResourceIds.generateOne
        val otherTsProject2Id = projectResourceIds.generateOne
        val tsInfoVisibility  = visibilitiesBroader(projects.Visibility.Private).generateOne
        val tsInfo =
          searchInfoObjectsGen(withLinkTo = project.resourceId, and = otherTsProject1Id, otherTsProject2Id).generateOne
            .copy(visibility = tsInfoVisibility)

        val infoSet = CalculatorInfoSet.TSInfoOnly(
          project,
          tsInfo,
          Map(project.resourceId -> projects.Visibility.Private,
              otherTsProject1Id  -> projects.Visibility.Private,
              otherTsProject2Id  -> tsInfoVisibility
          )
        )

        val expectedLinkToDelete = tsInfo
          .findLink(project.resourceId)
          .getOrElse(fail("There supposed to be a link to delete"))

        calculateCommands(infoSet) should produce(
          (expectedLinkToDelete.asQuads + linkEdge(tsInfo, expectedLinkToDelete)).map(Delete).toList
        )
      }

    "create Deletes for the project link and Delete & Insert for the info visibility " +
      "when the info found in the TS is associated also with other projects - the other projects' visibilities are narrower" in {

        val project = newProject()

        val otherTsProjectId         = projectResourceIds.generateOne
        val otherTsProjectVisibility = visibilitiesNarrower(projects.Visibility.Public).generateOne
        val tsInfo =
          searchInfoObjectsGen(withLinkTo = project.resourceId, and = otherTsProjectId).generateOne
            .copy(visibility = projects.Visibility.Public)

        val infoSet = CalculatorInfoSet.TSInfoOnly(
          project,
          tsInfo,
          Map(project.resourceId -> projects.Visibility.Public, otherTsProjectId -> otherTsProjectVisibility)
        )

        val expectedLinkToDelete = tsInfo
          .findLink(project.resourceId)
          .getOrElse(fail("There supposed to be a link to delete"))

        calculateCommands(infoSet) should produce(
          (expectedLinkToDelete.asQuads + linkEdge(tsInfo, expectedLinkToDelete)).map(Delete).toList :::
            Insert(visibilityQuad(tsInfo, otherTsProjectVisibility)) ::
            Delete(visibilityQuad(tsInfo, tsInfo.visibility)) :: Nil
        )
      }

    "return no commands " +
      "when the info found in the TS only but for other projects" in {

        val project = newProject()

        val otherTsProjectId         = projectResourceIds.generateOne
        val otherTsProjectVisibility = visibilitiesNarrower(projects.Visibility.Public).generateOne
        val tsInfo =
          searchInfoObjectsGen(withLinkTo = project.resourceId, and = otherTsProjectId).generateOne
            .copy(visibility = projects.Visibility.Public)

        val infoSet = CalculatorInfoSet.TSInfoOnly(
          project,
          tsInfo,
          Map(project.resourceId -> projects.Visibility.Public, otherTsProjectId -> otherTsProjectVisibility)
        )

        val expectedLinkToDelete = tsInfo
          .findLink(project.resourceId)
          .getOrElse(fail("There supposed to be a link to delete"))

        calculateCommands(infoSet) should produce(
          (expectedLinkToDelete.asQuads +
            DatasetsQuad(tsInfo.topmostSameAs,
                         SearchInfoOntology.linkProperty,
                         expectedLinkToDelete.resourceId.asEntityId
            )).map(Delete).toList :::
            Insert(visibilityQuad(tsInfo, otherTsProjectVisibility)) ::
            Delete(visibilityQuad(tsInfo, tsInfo.visibility)) :: Nil
        )
      }

    "fail for when input data in illegal state" in {

      val project = newProject()

      val otherTsProjectId         = projectResourceIds.generateOne
      val otherTsProjectVisibility = visibilitiesNarrower(projects.Visibility.Public).generateOne
      val tsInfo = searchInfoObjectsGen(withLinkTo = otherTsProjectId).generateOne
        .copy(visibility = otherTsProjectVisibility)

      val infoSet = CalculatorInfoSet.TSInfoOnly(
        project,
        tsInfo,
        Map(otherTsProjectId -> otherTsProjectVisibility)
      )

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

  private def visibilityQuad(info: SearchInfo, visibility: projects.Visibility): Quad =
    DatasetsQuad(info.topmostSameAs, SearchInfoOntology.visibilityProperty.id, visibility.asObject)

  private def produce(expected: List[UpdateCommand]) = new Matcher[Try[List[UpdateCommand]]] {
    def apply(actual: Try[List[UpdateCommand]]): MatchResult = {
      val matching = actual.map(_.toSet == expected.toSet).fold(fail(_), identity)
      MatchResult(matching, s"\nExpected: $expected\nActual: $actual", s"\nExpected: $expected\nActual: $actual")
    }
  }

  private def newProject(visibility: projects.Visibility = anyVisibility): entities.Project =
    anyRenkuProjectEntities(fixed(visibility)).map(_.to[entities.Project]).generateOne
}
