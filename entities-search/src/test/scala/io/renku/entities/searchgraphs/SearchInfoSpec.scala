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

import Generators._
import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.entities.searchgraphs.Link.{ImportedDataset, OriginalDataset, show}
import io.renku.entities.searchgraphs.SearchInfo.DateModified
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.model.GraphModelGenerators.{datasetCreatedDates, datasetExternalSameAs, datasetResourceIds, datasetTopmostSameAs, projectPaths, projectResourceIds, projectVisibilities}
import io.renku.graph.model.{datasets, projects}
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SearchInfoSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "visibility" should {

    "return max visibility from all the links" in {
      searchInfoObjectsGen
        .map(info =>
          info.copy(links =
            NonEmptyList.of(
              linkObjectsGen(info.topmostSameAs, fixed(projects.Visibility.Internal)).generateOne,
              linkObjectsGen(info.topmostSameAs, fixed(projects.Visibility.Private)).generateOne,
              linkObjectsGen(info.topmostSameAs, fixed(projects.Visibility.Public)).generateOne
            )
          )
        )
        .generateOne
        .visibility shouldBe projects.Visibility.Public
    }
  }

  "show" should {

    "return String representation of the Info" in {
      forAll(searchInfoObjectsGen) {
        case info @ SearchInfo(topSameAs,
                               name,
                               dateOriginal,
                               maybeDateModified,
                               creators,
                               keywords,
                               maybeDescription,
                               images,
                               links
            ) =>
          info.show shouldBe List(
            show"topmostSameAs = $topSameAs".some,
            show"name = $name".some,
            show"visibility = ${info.visibility}".some,
            dateOriginal match {
              case d: datasets.DateCreated   => show"dateCreated = $d".some
              case d: datasets.DatePublished => show"datePublished = $d".some
            },
            maybeDateModified.map(d => show"dateModified = $d"),
            show"creators = [${creators.map(_.show).intercalate("; ")}}]".some,
            keywords match {
              case Nil => None
              case k   => show"keywords = [${k.mkString(", ")}]".some
            },
            maybeDescription.map(d => show"description = $d"),
            images match {
              case Nil => None
              case i   => show"images = [${i.sortBy(_.position).map(i => show"${i.uri.value}").mkString(", ")}]".some
            },
            show"links = [${links.map(_.show).intercalate("; ")}}]".some
          ).flatten.mkString(", ")
      }
    }
  }
}

class DateModifiedSpec extends AnyWordSpec with should.Matchers {

  "DateModified.apply(DateCreated)" should {

    "instantiate DateModified from a Dataset DateCreated" in {
      val created = datasetCreatedDates().generateOne
      DateModified(created).value shouldBe created.instant
    }
  }
}

class LinkSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "apply" should {

    "return OriginalDataset if both the topmostSameAs and datasetId are equal" in {
      val datasetId     = datasetResourceIds.generateOne
      val topmostSameAs = datasets.TopmostSameAs(datasetId.asEntityId)
      val projectId     = projectResourceIds.generateOne
      val projectPath   = projectPaths.generateOne
      val visibility    = projectVisibilities.generateOne

      val link = Link(topmostSameAs, datasetId, projectId, projectPath, visibility)

      link            shouldBe a[OriginalDataset]
      link.resourceId shouldBe links.ResourceId.from(topmostSameAs, projectPath)
    }

    "return OriginalDataset if linkId starts with datasetId" in {
      val datasetId   = datasetResourceIds.generateOne
      val projectId   = projectResourceIds.generateOne
      val projectPath = projectPaths.generateOne
      val visibility  = projectVisibilities.generateOne
      val linkId      = links.ResourceId.from(datasets.TopmostSameAs(datasetId.asEntityId), projectPath)

      val link = Link(linkId, datasetId, projectId, visibility)

      link            shouldBe a[OriginalDataset]
      link.resourceId shouldBe linkId
      link.projectId  shouldBe projectId
      link.datasetId  shouldBe datasetId
      link.visibility shouldBe visibility
    }

    "return ImportedDataset if the topmostSameAs and datasetId are not equal" in {
      val datasetId     = datasetResourceIds.generateOne
      val topmostSameAs = datasetExternalSameAs.generateAs(datasets.TopmostSameAs(_))
      val projectId     = projectResourceIds.generateOne
      val projectPath   = projectPaths.generateOne
      val visibility    = projectVisibilities.generateOne

      val link = Link(topmostSameAs, datasetId, projectId, projectPath, visibility)

      link            shouldBe a[ImportedDataset]
      link.resourceId shouldBe links.ResourceId.from(topmostSameAs, projectPath)
    }

    "return ImportedDataset if linkId does not starts with datasetId" in {
      val datasetId   = datasetResourceIds.generateOne
      val projectId   = projectResourceIds.generateOne
      val projectPath = projectPaths.generateOne
      val linkId      = links.ResourceId.from(datasetExternalSameAs.generateAs(datasets.TopmostSameAs(_)), projectPath)
      val visibility  = projectVisibilities.generateOne

      val link = Link(linkId, datasetId, projectId, visibility)

      link            shouldBe a[ImportedDataset]
      link.resourceId shouldBe linkId
      link.projectId  shouldBe projectId
      link.datasetId  shouldBe datasetId
      link.visibility shouldBe visibility
    }
  }

  "show" should {

    "returns String representation" in {
      forAll(linkObjectsGen(datasetTopmostSameAs.generateOne)) { link =>
        link.show shouldBe show"id = ${link.resourceId}, projectId = ${link.projectId}, datasetId = ${link.datasetId}"
      }
    }
  }
}

class PersonLinkSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "show" should {

    "returns String representation" in {
      forAll(personInfos) { case info @ PersonInfo(id, name) =>
        info.show shouldBe show"id = $id, name = $name"
      }
    }
  }
}
