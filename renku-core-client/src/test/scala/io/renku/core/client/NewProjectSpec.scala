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

import Generators.newProjectsGen
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class NewProjectSpec extends AnyFlatSpec with should.Matchers with EitherValues {

  it should "encode to JSON" in {

    val newProject = newProjectsGen.generateOne

    newProject.asJson shouldBe
      json"""{
        "url":                 ${newProject.template.repositoryUrl},
        "identifier":          ${newProject.template.identifier},
        "project_repository":  ${newProject.projectRepository},
        "project_namespace":   ${newProject.namespace},
        "project_name":        ${newProject.name},
        "project_keywords":    ${newProject.keywords},
        "project_description": ${newProject.maybeDescription},
        "initial_branch":      ${newProject.branch}
      }""".dropNullValues
  }
}
