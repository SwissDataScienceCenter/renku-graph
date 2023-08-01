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

package io.renku.triplesgenerator.api.events

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import Generators._
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.{nonEmptyStrings, timestampsNotInTheFuture}
import io.renku.graph.model.projects
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class ProjectActivatedSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with EitherValues
    with MockFactory {

  "forProject" should {

    "instantiate a new event with the current timestamp" in {

      val currentTime = Instant.now()
      val now         = mockFunction[Instant]
      now.expects().returning(currentTime)

      val slug = projectSlugs.generateOne

      ProjectActivated.forProject(slug, now) shouldBe ProjectActivated(slug, currentTime)
    }
  }

  "json codec" should {

    "encode and decode" in {

      val event = projectActivatedEvents.generateOne

      event.asJson.hcursor.as[ProjectActivated].value shouldBe event
    }

    "be able to decode json valid from the contract point of view" in {
      json"""{
        "categoryName": "PROJECT_ACTIVATED",
        "project": {
          "slug": "project/path"
        },
        "date": "1988-11-04T00:00:00.000Z"
      }""".hcursor.as[ProjectActivated].value shouldBe ProjectActivated(
        projects.Slug("project/path"),
        ProjectActivated.DateActivated(Instant.parse("1988-11-04T00:00:00.000Z"))
      )
    }

    "fail if categoryName does not match" in {

      val otherCategory = nonEmptyStrings().generateOne
      val result = json"""{
        "categoryName": $otherCategory,
        "project": {
          "slug": ${projectSlugs.generateOne}
        },
        "date": ${timestampsNotInTheFuture.generateAs(ProjectActivated.DateActivated)}
      }""".hcursor.as[ProjectActivated]

      result.left.value.getMessage() should include(s"Expected PROJECT_ACTIVATED but got $otherCategory")
    }
  }

  "show" should {

    "return String info with slug and the date" in {

      val event = projectActivatedEvents.generateOne

      event.show shouldBe show"projectSlug = ${event.slug}, date = ${event.dateActivated}"
    }
  }
}
