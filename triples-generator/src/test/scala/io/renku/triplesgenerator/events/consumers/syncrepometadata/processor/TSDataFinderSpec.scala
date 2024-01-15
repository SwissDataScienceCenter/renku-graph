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

package io.renku.triplesgenerator.events.consumers.syncrepometadata.processor

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.triplesstore.{GraphJenaSpec, ProjectsConnectionConfig}
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class TSDataFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with should.Matchers
    with OptionValues
    with ScalaCheckPropertyChecks {

  forAll(anyProjectEntities.map(_.to[entities.Project])) { project =>
    it should s"fetch relevant data from the TS - project ${project.name}" in projectsDSConfig.use { implicit pcc =>
      uploadToProjects(project) >>
        finder
          .fetchTSData(project.slug)
          .asserting(
            _.value shouldBe DataExtract
              .TS(
                project.resourceId,
                project.slug,
                project.name,
                project.visibility,
                project.dateModified.some,
                project.maybeDescription,
                project.keywords,
                project.images.sortBy(_.position).map(_.uri)
              )
          )
    }
  }

  it should "return None if there's no project with the given slug" in projectsDSConfig.use { implicit pcc =>
    finder
      .fetchTSData(projectSlugs.generateOne)
      .asserting(_ shouldBe None)
  }

  private implicit val logger: TestLogger[IO] = TestLogger[IO]()
  private def finder(implicit pcc: ProjectsConnectionConfig) = new TSDataFinderImpl[IO](tsClient)
}
