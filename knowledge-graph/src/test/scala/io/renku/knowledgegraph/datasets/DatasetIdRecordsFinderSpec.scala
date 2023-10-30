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

package io.renku.knowledgegraph.datasets

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.triplesstore.ProjectSparqlClient

class DatasetIdRecordsFinderSpec extends SecurityRecordFinderSupport {
  lazy val finder = ProjectSparqlClient[IO](projectsDSConnectionInfo).map(DatasetIdRecordsFinder.apply[IO])

  it should "find security records for a simple project with one dataset" in {
    val project = projectWithDatasetAndMembers.generateOne

    val dsId = project.datasets.head.identification.identifier

    for {
      _ <- provisionTestProject(project)
      r <- finder.use(_.apply(dsId, None))
      _ = r shouldBe List(project).map(toSecRecord)
    } yield ()
  }

  it should "find security records without members if there are none" in {
    val project = projectWithDatasetAndNoMembers.generateOne
    val dsId    = project.datasets.head.identification.identifier
    for {
      _ <- provisionTestProject(project)
      r <- finder.use(_.apply(dsId, None))
      _ = r shouldBe List(project).map(toSecRecord)
    } yield ()
  }

  it should "find security records for a forked project" in {
    val (dataset, (parentProject, project)) = projectAndFork.generateOne

    val dsId = dataset.identification.identifier

    for {
      _ <- provisionTestProjects(project, parentProject)
      r <- finder.use(_.apply(dsId, None))
      _ = r shouldBe List(project, parentProject).map(toSecRecord).sortBy(_.projectSlug.value)
    } yield ()
  }

  it should "find security records for the project that has the dataset" in {
    val createProjects = projectWithDatasetAndMembers.asStream.toIO
      .evalTap(provisionTestProject(_))
      .take(3)
      .compile
      .toList

    for {
      projects <- createProjects
      dsId = projects.head.datasets.head.identification.identifier
      r <- finder.use(_.apply(dsId, None))
      _ = r shouldBe projects.take(1).map(toSecRecord)
    } yield ()
  }

  it should "find nothing if there is no project using the dataset" in {
    val project = projectWithDatasetAndMembers.generateOne
    val dsId = EntitiesGenerators.datasetIdentifiers
      .suchThat(_ != project.datasets.head.identification.identifier)
      .generateOne

    for {
      _ <- provisionTestProject(project)
      r <- finder.use(_.apply(dsId, None))
      _ = r shouldBe Nil
    } yield ()
  }
}
