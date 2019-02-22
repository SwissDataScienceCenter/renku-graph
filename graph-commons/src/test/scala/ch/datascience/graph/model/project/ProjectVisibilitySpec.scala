/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.model.project

import io.circe.{DecodingFailure, Json}
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class ProjectVisibilitySpec extends WordSpec {

  "ProjectVisibility" should {

    "define cases for 'private', 'public' and 'internal'" in {
      ProjectVisibility.all.map(_.value) should contain only ("private", "public", "internal")
    }
  }

  "projectVisibilityDecoder" should {

    ProjectVisibility.all foreach { visibility =>
      s"deserialize $visibility" in {
        Json.fromString(visibility.value).as[ProjectVisibility] shouldBe Right(visibility)
      }
    }

    "fail for unknown value" in {
      Json.fromString("unknown").as[ProjectVisibility] shouldBe Left(
        DecodingFailure(
          s"'unknown' is not a valid project visibility. Allowed values are: ${ProjectVisibility.all.mkString(", ")}",
          Nil
        )
      )
    }
  }
}
