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

package io.renku.knowledgegraph.projects.images

import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.images.ImageUri
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ImagesEncoderSpec extends AnyFlatSpec with should.Matchers with ScalaCheckPropertyChecks with ImagesEncoder {

  it should "encode the List[ImageUri] -> slug tuples to JSON" in {
    forAll(imageUris.toGeneratorOfList()) { images =>
      (images -> projectSlug).asJson shouldBe imagesEncoder(images)
    }
  }

  private lazy val imagesEncoder: Encoder[List[ImageUri]] = Encoder.instance[List[ImageUri]] { images =>
    Json.fromValues(
      images.map {
        case uri: ImageUri.Relative => json"""{
        "location": $uri,
        "_links": [{
          "rel":  "view",
          "href": ${s"$gitLabUrl/$projectSlug/raw/master/$uri"}
        }]
      }"""
        case uri: ImageUri.Absolute => json"""{
        "location": $uri,
        "_links": [{
          "rel":  "view",
          "href": $uri
        }]
      }"""
      }
    )
  }

  private lazy val projectSlug = projectSlugs.generateOne
  private implicit lazy val gitLabUrl: GitLabUrl = gitLabUrls.generateOne
}
