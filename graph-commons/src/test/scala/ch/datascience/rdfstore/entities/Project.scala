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

package ch.datascience.rdfstore.entities

import ch.datascience.graph.config.GitLabApiUrl
import ch.datascience.graph.model.projects.{DateCreated, Name, Path, ResourceId, Visibility}
import ch.datascience.graph.model.{CliVersion, SchemaVersion}

final case class Project(path:               Path,
                         name:               Name,
                         agent:              CliVersion,
                         dateCreated:        DateCreated,
                         maybeCreator:       Option[Person],
                         maybeVisibility:    Option[Visibility],
                         maybeParentProject: Option[Project],
                         members:            Set[Person],
                         version:            SchemaVersion
)

object Project {

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import JsonLDEncoder._
  import io.renku.jsonld.syntax._

  def apply(path:               Path,
            name:               Name,
            agent:              CliVersion,
            dateCreated:        DateCreated,
            maybeCreator:       Option[Person],
            schemaVersion:      SchemaVersion,
            maybeVisibility:    Option[Visibility] = None,
            maybeParentProject: Option[Project] = None,
            members:            Set[Person] = Set.empty
  ): Project = Project(
    path,
    name,
    agent,
    dateCreated,
    maybeCreator,
    maybeVisibility,
    maybeParentProject,
    members,
    schemaVersion
  )

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Project] =
    JsonLDEncoder.instance { project =>
      JsonLD.entity(
        project.asEntityId,
        EntityTypes.of(prov / "Location", schema / "Project"),
        schema / "name"             -> project.name.asJsonLD,
        schema / "agent"            -> project.agent.asJsonLD,
        schema / "dateCreated"      -> project.dateCreated.asJsonLD,
        schema / "creator"          -> project.maybeCreator.asJsonLD,
        renku / "projectVisibility" -> project.maybeVisibility.asJsonLD,
        schema / "member"           -> project.members.toList.asJsonLD,
        schema / "schemaVersion"    -> project.version.asJsonLD,
        prov / "wasDerivedFrom"     -> project.maybeParentProject.asJsonLD
      )
    }

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Project] =
    EntityIdEncoder.instance(project => renkuBaseUrl / "projects" / project.path)

  private implicit val projectResourceToEntityId: ResourceId => EntityId =
    resource => EntityId of resource.value
}
