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

package io.renku.core.client

import Generators.projectUpdatesGen
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.{projectDescriptions, projectKeywords}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ProjectUpdatesSpec extends AnyFlatSpec with should.Matchers with EitherValues {

  it should "encode to JSON - case with no update on description and keywords" in {

    val updates = projectUpdatesGen.generateOne.copy(maybeDescription = None, maybeKeywords = None)
    updates.asJson shouldBe json"""{
      "git_url":         ${updates.projectUrl},
      "is_delayed":      false,
      "migrate_project": false
    }"""
  }

  it should "encode to JSON - case with an update on description to a non-blank value" in {

    val newDesc = projectDescriptions.generateOne
    val updates = projectUpdatesGen.generateOne.copy(maybeDescription = newDesc.some.some, maybeKeywords = None)
    updates.asJson shouldBe json"""{
      "git_url":         ${updates.projectUrl},
      "is_delayed":      false,
      "migrate_project": false,
      "description":     $newDesc
    }"""
  }

  it should "encode to JSON - case with an update on description to remove it" in {

    val updates = projectUpdatesGen.generateOne.copy(maybeDescription = Some(None), maybeKeywords = None)
    updates.asJson shouldBe json"""{
      "git_url":         ${updates.projectUrl},
      "is_delayed":      false,
      "migrate_project": false,
      "description":     ""
    }"""
  }

  it should "encode to JSON - case with an update on keywords to a non-blank values" in {

    val newKeywords = projectKeywords.generateSet(min = 1)
    val updates     = projectUpdatesGen.generateOne.copy(maybeDescription = None, maybeKeywords = newKeywords.some)
    updates.asJson shouldBe
      json"""{
      "git_url":         ${updates.projectUrl},
      "is_delayed":      false,
      "migrate_project": false,
      "keywords":        ${newKeywords.toList}
    }"""
  }

  it should "encode to JSON - case with an update on keywords to remove them" in {

    val updates = projectUpdatesGen.generateOne.copy(maybeDescription = None, maybeKeywords = Some(Set.empty))
    updates.asJson shouldBe
      json"""{
      "git_url":         ${updates.projectUrl},
      "is_delayed":      false,
      "migrate_project": false,
      "keywords":        []
    }"""
  }
}
