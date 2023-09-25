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

package io.renku.triplesgenerator.api

import Generators.newProjectsGen
import cats.syntax.all._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.{imageUris, projectDescriptions, projectKeywords}
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class NewProjectSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks with EitherValues {

  "encode/decode" should {

    "work for any NewProject " in {
      forAll(newProjectsGen) { newProject =>
        newProject.asJson.hcursor.as[NewProject].value shouldBe newProject
      }
    }
  }

  "show" should {

    "not print values that are empty or none" in {

      val newProject = newProjectsGen.generateOne.copy(maybeDescription = None, keywords = Set.empty, images = Nil)

      newProject.show shouldBe s"name=${newProject.name}, " +
        s"slug=${newProject.slug}, " +
        s"dateCreated=${newProject.dateCreated}, " +
        s"creator=(name=${newProject.creator.name}, id=${newProject.creator.id}), " +
        s"visibility=${newProject.visibility}"
    }

    "print all set values" in {

      val desc = projectDescriptions.generateOne
      val newProject = newProjectsGen.generateOne.copy(maybeDescription = desc.some,
                                                       keywords = projectKeywords.generateSet(min = 1),
                                                       images = imageUris.generateNonEmptyList().toList
      )

      newProject.show shouldBe s"name=${newProject.name}, " +
        s"slug=${newProject.slug}, " +
        s"description=$desc, " +
        s"dateCreated=${newProject.dateCreated}, " +
        s"creator=(name=${newProject.creator.name}, id=${newProject.creator.id}), " +
        s"keywords=[${newProject.keywords.mkString(", ")}], " +
        s"images=[${newProject.images.mkString(", ")}], " +
        s"visibility=${newProject.visibility}"
    }
  }
}
