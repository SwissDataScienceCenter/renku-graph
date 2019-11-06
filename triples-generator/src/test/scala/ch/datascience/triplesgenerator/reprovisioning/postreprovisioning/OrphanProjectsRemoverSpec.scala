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

package ch.datascience.triplesgenerator.reprovisioning.postreprovisioning

import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators.{emails, names}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.{projectCreatedDates, projectNames, projectPaths}
import ch.datascience.graph.model.projects.{FullProjectPath, ProjectPath}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples.entities.{Person, Project}
import ch.datascience.rdfstore.triples.{renkuBaseUrl, singleFileAndCommitWithDataset, triples}
import ch.datascience.triplesgenerator.reprovisioning.IORdfStoreUpdater
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class OrphanProjectsRemoverSpec extends WordSpec with InMemoryRdfStore {

  "run" should {

    "remove project triples if the project is not used anywhere" in new TestCase {

      val project             = projectPaths.generateOne
      val projectCreatorName  = names.generateOne
      val projectCreatorEmail = emails.generateOne
      val projectCreatorId    = Person.Id(projectCreatorName)

      loadToStore(
        triples(
          Project(Project.Id(renkuBaseUrl, project),
                  projectNames.generateOne,
                  projectCreatedDates.generateOne,
                  projectCreatorId) +:
            Person(projectCreatorId, Some(projectCreatorEmail)) +:
            singleFileAndCommitWithDataset(projectPaths.generateOne)
        )
      )

      triplesFor(project).size should be > 0

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      triplesFor(project) shouldBe empty
    }

    "do nothing if the project is used in some other entity" in new TestCase {

      val project             = projectPaths.generateOne
      val projectCreatorName  = names.generateOne
      val projectCreatorEmail = emails.generateOne
      val projectCreatorId    = Person.Id(projectCreatorName)

      loadToStore(
        triples(
          Project(Project.Id(renkuBaseUrl, project),
                  projectNames.generateOne,
                  projectCreatedDates.generateOne,
                  projectCreatorId) +:
            Person(projectCreatorId, Some(projectCreatorEmail)) +:
            singleFileAndCommitWithDataset(project)
        )
      )

      val projectTriples = triplesFor(project)
      projectTriples.size should be > 0

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      triplesFor(project) should have size projectTriples.size
    }
  }

  private trait TestCase {
    val triplesRemover = new IORdfStoreUpdater(rdfStoreConfig, TestLogger()) with OrphanProjectsRemover[IO]
  }

  private def triplesFor(project: ProjectPath) =
    runQuery {
      s"""|SELECT ?p 
          |WHERE { ${FullProjectPath(renkuBaseUrl, project).showAs[RdfResource]} ?o ?p }""".stripMargin
    }.unsafeRunSync()
      .map(row => row("p"))
      .toSet
}
