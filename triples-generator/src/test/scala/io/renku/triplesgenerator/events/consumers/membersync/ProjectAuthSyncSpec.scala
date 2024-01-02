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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.projects
import io.renku.projectauth.{Generators, ProjectAuthData, ProjectAuthService}
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ProjectAuthSyncSpec extends AnyWordSpec with should.Matchers with TryValues with MockFactory {

  "syncProject" should {

    "call the ProjectAuthService.update" in {

      val authData = Generators.projectAuthDataGen.generateOne
      givenAuthDataUpdate(authData, returning = ().pure[Try])

      projectAuthSync.syncProject(authData).success.value shouldBe ()
    }
  }

  "removeAuthData" should {

    "call the ProjectAuthService.remove" in {

      val slug = projectSlugs.generateOne
      givenAuthDataRemove(slug, returning = ().pure[Try])

      projectAuthSync.removeAuthData(slug).success.value shouldBe ()
    }
  }

  private val projectAuthService: ProjectAuthService[Try] = mock[ProjectAuthService[Try]]
  private lazy val projectAuthSync = new ProjectAuthSync.Impl[Try](projectAuthService)

  private def givenAuthDataUpdate(data: ProjectAuthData, returning: Try[Unit]) =
    (projectAuthService.update _)
      .expects(data)
      .returning(returning)

  private def givenAuthDataRemove(slug: projects.Slug, returning: Try[Unit]) =
    (projectAuthService
      .remove(_: NonEmptyList[projects.Slug]))
      .expects(NonEmptyList.of(slug))
      .returning(returning)
}
