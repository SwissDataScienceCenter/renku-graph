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

package ch.datascience.rdfstore.triples

import ch.datascience.generators.CommonGraphGenerators.{emails, names, schemaVersions}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.EventsGenerators.committedDates
import ch.datascience.graph.model.GraphModelGenerators.{projectCreatedDates, projectNames}
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.{FilePath, ProjectPath}
import ch.datascience.graph.model.users.{Email, Name => UserName}
import ch.datascience.graph.model.{SchemaVersion, projects, users}
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.triples.entities._
import io.circe.Json

object singleFileAndCommit {

  def apply(projectPath:        ProjectPath,
            projectName:        projects.Name = projectNames.generateOne,
            projectDateCreated: projects.DateCreated = projectCreatedDates.generateOne,
            projectCreator:     (users.Name, Email) = names.generateOne -> emails.generateOne,
            commitId:           CommitId,
            committerName:      UserName = names.generateOne,
            committerEmail:     Email = emails.generateOne,
            schemaVersion:      SchemaVersion = schemaVersions.generateOne,
            renkuBaseUrl:       RenkuBaseUrl = renkuBaseUrl)(implicit fusekiBaseUrl: FusekiBaseUrl): List[Json] = {
    val filePath                                  = FilePath("README.md")
    val generationPath                            = FilePath("tree") / filePath
    val projectId                                 = Project.Id(renkuBaseUrl, projectPath)
    val (projectCreatorName, projectCreatorEmail) = projectCreator
    val projectCreatorId                          = Person.Id(projectCreatorName)
    val commitGenerationId                        = CommitGeneration.Id(commitId, generationPath)
    val commitActivityId                          = CommitActivity.Id(commitId)
    val committerPersonId                         = Person.Id(committerName)
    val agentId                                   = Agent.Id(schemaVersion)

    List(
      Project(projectId, projectName, projectDateCreated, projectCreatorId),
      Person(projectCreatorId, Some(projectCreatorEmail)),
      CommitEntity(CommitEntity.Id(commitId, filePath), projectId, commitGenerationId),
      CommitActivity(commitActivityId,
                     projectId,
                     committedDates.generateOne,
                     agentId,
                     committerPersonId,
                     maybeInfluencedBy = Nil),
      Person(committerPersonId, Some(committerEmail)),
      CommitGeneration(commitGenerationId, commitActivityId),
      Agent(agentId)
    )
  }
}
