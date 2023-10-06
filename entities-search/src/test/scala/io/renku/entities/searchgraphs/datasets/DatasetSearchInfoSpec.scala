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

package io.renku.entities.searchgraphs.datasets

import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.entities.searchgraphs.datasets.Generators._
import io.renku.entities.searchgraphs.datasets.Link.{ImportedDataset, OriginalDataset, show}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{datasetExternalSameAs, datasetResourceIds, datasetTopmostSameAs, projectSlugs, projectResourceIds}
import io.renku.graph.model.datasets
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DatasetSearchInfoSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "findLink" should {

    "return the relevant link if there's a link for the given projectId" in {

      val info         = datasetSearchInfoObjects.generateOne
      val project1Link = linkObjectsGen(info.topmostSameAs).generateOne
      val project2Link = linkObjectsGen(info.topmostSameAs).generateOne
      val someLink     = Gen.oneOf(project1Link, project2Link).generateOne

      info.copy(links = NonEmptyList.of(project1Link, project2Link)).findLink(someLink.projectId) shouldBe
        someLink.some
    }

    "return None if there's no link for the given projectId" in {
      datasetSearchInfoObjects.generateOne.findLink(projectResourceIds.generateOne) shouldBe None
    }
  }

  "show" should {

    "return String representation of the Info" in {
      forAll(datasetSearchInfoObjects) {
        case info @ DatasetSearchInfo(topSameAs,
                                      name,
                                      slug,
                                      visibility,
                                      createdOrPublished,
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
            show"slug = $slug".some,
            show"visibility = $visibility".some,
            createdOrPublished match {
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

class LinkSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "apply" should {

    "return OriginalDataset if both the topmostSameAs and datasetId are equal" in {
      val datasetId     = datasetResourceIds.generateOne
      val topmostSameAs = datasets.TopmostSameAs(datasetId.asEntityId)
      val projectId     = projectResourceIds.generateOne
      val projectSlug   = projectSlugs.generateOne

      val link = Link(topmostSameAs, datasetId, projectId, projectSlug)

      link            shouldBe a[OriginalDataset]
      link.resourceId shouldBe links.ResourceId.from(topmostSameAs, projectSlug)
    }

    "return OriginalDataset if linkId starts with datasetId" in {
      val datasetId   = datasetResourceIds.generateOne
      val projectId   = projectResourceIds.generateOne
      val projectSlug = projectSlugs.generateOne
      val linkId      = links.ResourceId.from(datasets.TopmostSameAs(datasetId.asEntityId), projectSlug)

      val link = Link(linkId, datasetId, projectId)

      link            shouldBe a[OriginalDataset]
      link.resourceId shouldBe linkId
      link.projectId  shouldBe projectId
      link.datasetId  shouldBe datasetId
    }

    "return ImportedDataset if the topmostSameAs and datasetId are not equal" in {
      val datasetId     = datasetResourceIds.generateOne
      val topmostSameAs = datasetExternalSameAs.generateAs(datasets.TopmostSameAs(_))
      val projectId     = projectResourceIds.generateOne
      val projectSlug   = projectSlugs.generateOne

      val link = Link(topmostSameAs, datasetId, projectId, projectSlug)

      link            shouldBe a[ImportedDataset]
      link.resourceId shouldBe links.ResourceId.from(topmostSameAs, projectSlug)
    }

    "return ImportedDataset if linkId does not starts with datasetId" in {
      val datasetId   = datasetResourceIds.generateOne
      val projectId   = projectResourceIds.generateOne
      val projectSlug = projectSlugs.generateOne
      val linkId      = links.ResourceId.from(datasetExternalSameAs.generateAs(datasets.TopmostSameAs(_)), projectSlug)

      val link = Link(linkId, datasetId, projectId)

      link            shouldBe a[ImportedDataset]
      link.resourceId shouldBe linkId
      link.projectId  shouldBe projectId
      link.datasetId  shouldBe datasetId
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
