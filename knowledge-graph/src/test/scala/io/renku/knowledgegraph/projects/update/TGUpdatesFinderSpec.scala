/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import io.renku.core.client.Generators.branches
import io.renku.generators.Generators.Implicits._
import io.renku.triplesgenerator.api.{ProjectUpdates => TGProjectUpdates}
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Try

class TGUpdatesFinderSpec extends AnyWordSpec with should.Matchers with TryValues with ScalaCheckPropertyChecks {

  private val updatesFinder = TGUpdatesFinder[Try]
  import updatesFinder.findTGProjectUpdates

  "findTGProjectUpdates - case without the Core update" should {

    "fail if no info about GL updated project given" in {

      val updates = projectUpdatesGen.suchThat(u => u.newImage.isEmpty && u.onlyGLUpdateNeeded).generateOne

      findTGProjectUpdates(updates, maybeGLUpdatedProject = None).failure.exception.getMessage shouldBe
        "No info about values updated in GL"
    }

    "merge the Project updates with the GL updated project " +
      "if info about GL updated project given" in {

        val updates   = projectUpdatesGen.suchThat(u => u.newImage.flatten.nonEmpty && u.onlyGLUpdateNeeded).generateOne
        val glUpdated = glUpdatedProjectsGen.suchThat(_.image.nonEmpty).generateOne

        findTGProjectUpdates(updates, glUpdated.some).success.value shouldBe TGProjectUpdates(
          newDescription = None,
          newImages = glUpdated.image.toList.some,
          newKeywords = None,
          newVisibility = updates.newVisibility
        )
      }

    "fail if there's a new image update but GL updated values without the uri" in {

      val updates   = projectUpdatesGen.suchThat(u => u.newImage.flatten.nonEmpty && u.onlyGLUpdateNeeded).generateOne
      val glUpdated = glUpdatedProjectsGen.suchThat(_.image.isEmpty).generateSome

      findTGProjectUpdates(updates, glUpdated).failure.exception.getMessage shouldBe
        "Image not updated in GL"
    }

    "fail if there's an image deletion but GL updated project contains some uri" in {

      val updates   = projectUpdatesGen.generateOne.copy(newImage = Some(None))
      val glUpdated = glUpdatedProjectsGen.suchThat(_.image.nonEmpty).generateSome

      findTGProjectUpdates(updates, glUpdated).failure.exception.getMessage shouldBe
        "Image not deleted in GL"
    }
  }

  "findTGProjectUpdates - case with the Core update" should {

    "fail there are GL updatable values " +
      "but no info about GL updated project given" in {

        val updates = projectUpdatesGen.suchThat(u => u.glUpdateNeeded && u.coreUpdateNeeded).generateOne
        val branch  = branches.generateOne

        findTGProjectUpdates(updates,
                             maybeGLUpdatedProject = None,
                             maybeDefaultBranch = DefaultBranch.Unprotected(branch).some,
                             corePushBranch = branch
        ).failure.exception.getMessage shouldBe "No info about values updated in GL"
      }

    "map the Core updatable values from the Project updates " +
      "if there are only Core updatable values and " +
      "it's confirmed Core pushed to the default branch" in {

        forAll(projectUpdatesGen.suchThat(_.coreUpdateNeeded).map(_.copy(newImage = None, newVisibility = None))) {
          updates =>
            val branch = branches.generateOne

            findTGProjectUpdates(updates,
                                 maybeGLUpdatedProject = None,
                                 maybeDefaultBranch = DefaultBranch.Unprotected(branch).some,
                                 corePushBranch = branch
            ).success.value shouldBe TGProjectUpdates(
              newDescription = updates.newDescription,
              newImages = None,
              newKeywords = updates.newKeywords,
              newVisibility = None
            )
        }
      }

    "return an empty TG updates object " +
      "if there are only Core updatable values and " +
      "it's confirmed Core pushed to a non default branch" in {

        val updates = projectUpdatesGen
          .suchThat(_.coreUpdateNeeded)
          .map(_.copy(newImage = None, newVisibility = None))
          .generateOne
        val branch = branches.generateOne

        DefaultBranch.PushProtected(branch).some :: Option.empty[DefaultBranch] :: Nil foreach { maybeDefaultBranch =>
          findTGProjectUpdates(updates,
                               maybeGLUpdatedProject = None,
                               maybeDefaultBranch,
                               corePushBranch = branch
          ).success.value shouldBe TGProjectUpdates.empty
        }
      }

    "merge the Project updates with the GL updated values " +
      "when info about GL updated project given and " +
      "it's confirmed Core pushed to the default branch" in {

        val updates   = projectUpdatesGen.suchThat(u => u.newImage.flatten.nonEmpty && u.coreUpdateNeeded).generateOne
        val glUpdated = glUpdatedProjectsGen.suchThat(_.image.nonEmpty).generateOne
        val branch    = branches.generateOne

        findTGProjectUpdates(updates,
                             glUpdated.some,
                             maybeDefaultBranch = DefaultBranch.Unprotected(branch).some,
                             corePushBranch = branch
        ).success.value shouldBe TGProjectUpdates(
          newDescription = updates.newDescription,
          newImages = glUpdated.image.toList.some,
          newKeywords = updates.newKeywords,
          newVisibility = updates.newVisibility
        )
      }

    "merge the Project updates with the GL updated values and " +
      "clear the Core updatable values " +
      "when info about GL updated project given and " +
      "it's confirmed Core pushed to a non default branch" in {

        val updates   = projectUpdatesGen.suchThat(u => u.newImage.flatten.nonEmpty && u.coreUpdateNeeded).generateOne
        val glUpdated = glUpdatedProjectsGen.suchThat(_.image.nonEmpty).generateOne
        val branch    = branches.generateOne

        DefaultBranch.PushProtected(branch).some :: Option.empty[DefaultBranch] :: Nil foreach { maybeDefaultBranch =>
          findTGProjectUpdates(updates,
                               glUpdated.some,
                               maybeDefaultBranch,
                               corePushBranch = branch
          ).success.value shouldBe TGProjectUpdates(
            newDescription = None,
            newImages = glUpdated.image.toList.some,
            newKeywords = None,
            newVisibility = updates.newVisibility
          )
        }
      }
  }
}
