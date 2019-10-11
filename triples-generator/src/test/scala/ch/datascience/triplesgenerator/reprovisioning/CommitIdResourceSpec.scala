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

package ch.datascience.triplesgenerator.reprovisioning

import ReProvisioningGenerators._
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.views.RdfResource
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.{Success, Try}

class CommitIdResourceSpec extends WordSpec with ScalaCheckPropertyChecks {

  "apply" should {

    "be instantiatable for a valid sha" in {
      forAll(httpUrls, shas) { (url, sha) =>
        val resource = s"$url/commit/$sha"
        CommitIdResource(resource).toString shouldBe resource
      }
    }

    "throw an IllegalArgumentException for non-sha values" in {
      intercept[IllegalArgumentException] {
        CommitIdResource("abc")
      }.getMessage shouldBe "'abc' is not a valid Commit Id Resource"
    }

    "throw an IllegalArgumentException for a blank value" in {
      intercept[IllegalArgumentException] {
        CommitIdResource("   ")
      }.getMessage shouldBe "'   ' is not a valid Commit Id Resource"
    }

    "throw an IllegalArgumentException for a sole commit sha" in {
      val sha = shas.generateOne
      intercept[IllegalArgumentException] {
        CommitIdResource(sha)
      }.getMessage shouldBe s"'$sha' is not a valid Commit Id Resource"
    }
  }

  "toCommitId" should {

    "return a valid CommitId - a case with no path after the commitId" in {
      forAll(httpUrls, commitIds) { (url, commitId) =>
        CommitIdResource(s"$url/commit/$commitId").as[Try, CommitId] shouldBe Success(commitId)
      }
    }

    "return a valid CommitId - case with some path after the commitId" in {
      forAll(httpUrls, commitIds, relativePaths()) { (url, commitId, path) =>
        CommitIdResource(s"$url/commit/$commitId/$path").as[Try, CommitId] shouldBe Success(commitId)
      }
    }
  }

  "rdfResourceRenderer" should {

    "wrap the value into <>" in {
      val commitIdResource = commitIdResources().generateOne
      commitIdResource.showAs[RdfResource] shouldBe s"<$commitIdResource>"
    }
  }
}
