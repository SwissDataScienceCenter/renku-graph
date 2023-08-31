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

package io.renku.knowledgegraph.projects.update

import Generators._
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.triplesgenerator.api.{ProjectUpdates => TGProjectUpdates}
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.util.Try

class TGUpdatesFinderSpec extends AnyFlatSpec with should.Matchers with TryValues {

  private val updatesFinder = TGUpdatesFinder[Try]
  import updatesFinder.findTGProjectUpdates

  it should "map values from the Project updates if no GL updated values given" in {

    val updates = projectUpdatesGen.suchThat(_.newImage.isEmpty).generateOne

    findTGProjectUpdates(updates, maybeGLUpdatedProject = None).success.value shouldBe TGProjectUpdates(
      newDescription = updates.newDescription,
      newImages = None,
      newKeywords = updates.newKeywords,
      newVisibility = updates.newVisibility
    )
  }

  it should "map values from the Project updates taking the image uri from GL update" in {

    val updates   = projectUpdatesGen.suchThat(_.newImage.flatten.nonEmpty).generateOne
    val glUpdated = glUpdatedProjectsGen.suchThat(_.image.nonEmpty).generateOne

    findTGProjectUpdates(updates, glUpdated.some).success.value shouldBe TGProjectUpdates(
      newDescription = updates.newDescription,
      newImages = glUpdated.image.toList.some,
      newKeywords = updates.newKeywords,
      newVisibility = updates.newVisibility
    )
  }

  it should "fail if there's a new image update but no GL updated values" in {

    val updates = projectUpdatesGen.suchThat(_.newImage.flatten.nonEmpty).generateOne

    findTGProjectUpdates(updates, maybeGLUpdatedProject = None).failure.exception.getMessage shouldBe
      "No info about updated values in GL"
  }

  it should "fail if there's a new image update but GL updated values without the uri" in {

    val updates   = projectUpdatesGen.suchThat(_.newImage.flatten.nonEmpty).generateOne
    val glUpdated = glUpdatedProjectsGen.suchThat(_.image.isEmpty).generateSome

    findTGProjectUpdates(updates, glUpdated).failure.exception.getMessage shouldBe
      "Image not updated in GL"
  }

  it should "fail if there's an image deletion but no GL updated values" in {

    val updates = projectUpdatesGen.generateOne.copy(newImage = Some(None))

    findTGProjectUpdates(updates, maybeGLUpdatedProject = None).failure.exception.getMessage shouldBe
      "No info about updated values in GL"
  }

  it should "fail if there's an image deletion but GL updated values contain some uri" in {

    val updates   = projectUpdatesGen.generateOne.copy(newImage = Some(None))
    val glUpdated = glUpdatedProjectsGen.suchThat(_.image.nonEmpty).generateSome

    findTGProjectUpdates(updates, glUpdated).failure.exception.getMessage shouldBe
      "Image not deleted in GL"
  }
}
