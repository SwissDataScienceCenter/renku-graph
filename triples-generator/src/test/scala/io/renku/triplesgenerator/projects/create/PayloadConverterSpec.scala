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

package io.renku.triplesgenerator.projects.create

import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.renkuUrls
import io.renku.graph.model.images.Image
import io.renku.graph.model.{RenkuUrl, entities, projects}
import io.renku.triplesgenerator.api.Generators.newProjectsGen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PayloadConverterSpec extends AnyFlatSpec with should.Matchers with ScalaCheckPropertyChecks {

  private implicit val renkuUrl: RenkuUrl = renkuUrls.generateOne
  private val converter = new PayloadConverterImpl

  it should s"turn the NewProject into a Project entity" in {
    forAll(newProjectsGen) { newProject =>
      val resourceId = projects.ResourceId(newProject.slug)
      val creator    = entities.Person(newProject.creator.name, newProject.creator.id)

      converter(newProject) shouldBe entities.NonRenkuProject.WithoutParent(
        resourceId,
        newProject.slug,
        newProject.name,
        newProject.maybeDescription,
        newProject.dateCreated,
        projects.DateModified(newProject.dateCreated.value),
        Some(creator),
        newProject.visibility,
        newProject.keywords,
        members = Set(entities.Project.Member(creator, newProject.creator.role)),
        Image.projectImage(resourceId, newProject.images)
      )
    }
  }
}
