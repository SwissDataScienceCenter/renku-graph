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

import Generators._
import cats.syntax.all._
import io.renku.graph.model.datasets
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ModelDatasetSearchInfoSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "show" should {

    "return String representation of the Info" in {
      forAll(modelDatasetSearchInfoObjects) {
        case info @ ModelDatasetSearchInfo(topSameAs,
                                           name,
                                           createdOrPublished,
                                           maybeDateModified,
                                           creators,
                                           keywords,
                                           maybeDescription,
                                           images,
                                           link
            ) =>
          info.show shouldBe List(
            show"topmostSameAs = $topSameAs".some,
            show"name = $name".some,
            show"visibility = ${link.visibility}".some,
            createdOrPublished match {
              case d: datasets.DateCreated   => show"dateCreated = $d".some
              case d: datasets.DatePublished => show"datePublished = $d".some
            },
            maybeDateModified.map(d => show"dateModified = $d"),
            show"creators = [${creators.mkString_("; ")}]".some,
            keywords match {
              case Nil => None
              case k   => show"keywords = [${k.mkString("; ")}]".some
            },
            maybeDescription.map(d => show"description = $d"),
            images match {
              case Nil => None
              case i   => show"images = [${i.sortBy(_.position).map(_.uri).mkString_("; ")}]".some
            },
            show"link = $link".some
          ).flatten.mkString(", ")
      }
    }
  }
}
