/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.datasets

import ch.datascience.generators.CommonGraphGenerators.cliVersions
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonEmptyStrings
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.datasets.{Identifier, TopmostDerivedFrom}
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators.addedToProjectObjects
import ch.datascience.knowledgegraph.datasets.model.DatasetProject
import ch.datascience.rdfstore.entities.EntitiesGenerators.persons
import ch.datascience.rdfstore.entities.{Activity, Agent, InvalidationEntity, Project}
import org.scalacheck.Gen

object EntityGenerators {
  implicit class ProjectOps(project: Project) {
    lazy val toDatasetProject: DatasetProject =
      DatasetProject(project.path, project.name, addedToProjectObjects.generateOne)
  }

  private def activities(project: Project): Gen[Activity] = for {
    commitId      <- commitIds
    committedDate <- committedDates
    committer     <- persons
    cliVersion    <- cliVersions
    comment       <- nonEmptyStrings()
  } yield Activity(
    commitId,
    committedDate,
    committer,
    project,
    Agent(cliVersion),
    comment,
    None,
    None,
    None,
    Nil
  )

  def invalidationEntity(datasetId:          Identifier,
                         project:            Project,
                         topmostDerivedFrom: Option[TopmostDerivedFrom] = None
  ): Gen[InvalidationEntity] = for {
    activity <- activities(project)
  } yield InvalidationEntity(
    activity.commitId,
    topmostDerivedFrom.map(_.value).getOrElse(datasetId.value),
    project,
    activity
  )
}
