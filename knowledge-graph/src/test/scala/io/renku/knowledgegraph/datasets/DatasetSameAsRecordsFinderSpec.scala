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
import io.renku.graph.model.datasets.SameAs
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.triplesstore.ProjectSparqlClient

class DatasetSameAsRecordsFinderSpec extends SecurityRecordFinderSupport {

  lazy val finder = ProjectSparqlClient[IO](projectsDSConnectionInfo).map(DatasetSameAsRecordsFinder.apply[IO])

  it should "find security records for a simple project with one dataset" in {
    val project = projectWithDatasetAndMembers.generateOne

    val dsSameAs = SameAs(project.datasets.head.provenance.topmostSameAs.value)

    for {
      _ <- provisionTestProject(project)
      r <- finder.use(_.apply(dsSameAs, None))
      _ = r shouldBe List(project).map(toSecRecord)
    } yield ()
  }

  it should "find security records without members if there are none" in {
    val project  = projectWithDatasetAndNoMembers.generateOne
    val dsSameAs = SameAs(project.datasets.head.provenance.topmostSameAs.value)
    for {
      _ <- provisionTestProject(project)
      r <- finder.use(_.apply(dsSameAs, None))
      _ = r shouldBe List(project).map(toSecRecord)
    } yield ()
  }

  it should "find security records for a forked project" in {
    val (dataset, (parentProject, project)) = projectAndFork.generateOne

    val dsSameAs = SameAs(dataset.provenance.topmostSameAs.value)

    for {
      _ <- provisionTestProjects(project, parentProject)
      r <- finder.use(_.apply(dsSameAs, None))
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
      dsSameAs = projects.head.datasets.head.provenance.topmostSameAs
      r <- finder.use(_.apply(SameAs(dsSameAs.value), None))
      _ = r shouldBe projects.take(1).map(toSecRecord)
    } yield ()
  }

  it should "find nothing if there is no project using the dataset" in {
    val project = projectWithDatasetAndMembers.generateOne
    val dsSameAs = EntitiesGenerators.datasetSameAs
      .suchThat(_.value != project.datasets.head.provenance.topmostSameAs.value)
      .generateOne

    for {
      _ <- provisionTestProject(project)
      r <- finder.use(_.apply(dsSameAs, None))
      _ = r shouldBe Nil
    } yield ()
  }
}
