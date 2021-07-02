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

package ch.datascience.graph.model.entities

import ch.datascience.graph.model.projects._
import ch.datascience.graph.model.{CliVersion, GitLabApiUrl, SchemaVersion}

sealed trait Project[+FC <: ForksCount] extends Project.ProjectOps[FC] {
  val resourceId:   ResourceId
  val path:         Path
  val name:         Name
  val agent:        CliVersion
  val dateCreated:  DateCreated
  val maybeCreator: Option[Person]
  val visibility:   Visibility
  val forksCount:   FC
  val members:      Set[Person]
  val version:      SchemaVersion
}

final case class ProjectWithoutParent[+FC <: ForksCount](resourceId:   ResourceId,
                                                         path:         Path,
                                                         name:         Name,
                                                         agent:        CliVersion,
                                                         dateCreated:  DateCreated,
                                                         maybeCreator: Option[Person],
                                                         visibility:   Visibility,
                                                         forksCount:   FC,
                                                         members:      Set[Person],
                                                         version:      SchemaVersion
) extends Project[FC]

final case class ProjectWithParent[+FC <: ForksCount](resourceId:   ResourceId,
                                                      path:         Path,
                                                      name:         Name,
                                                      agent:        CliVersion,
                                                      dateCreated:  DateCreated,
                                                      maybeCreator: Option[Person],
                                                      visibility:   Visibility,
                                                      forksCount:   FC,
                                                      members:      Set[Person],
                                                      version:      SchemaVersion,
                                                      parent:       Project[ForksCount.NonZero]
) extends Project[FC]

object Project {
  trait ProjectOps[+FC <: ForksCount] {
    self: Project[FC] =>

    lazy val topAncestorDateCreated: DateCreated = this match {
      case project: ProjectWithParent[_] => project.parent.topAncestorDateCreated
      case project => project.dateCreated
    }
  }

  import ch.datascience.graph.model.Schemas._
  import ch.datascience.graph.model.views.TinyTypeJsonLDEncoders._
  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

  implicit def encoder[P <: Project[_]](implicit gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[P] =
    JsonLDEncoder.instance {
      case project: ProjectWithParent[_] =>
        JsonLD.entity(
          project.resourceId.asEntityId,
          EntityTypes.of(prov / "Location", schema / "Project"),
          schema / "name"             -> project.name.asJsonLD,
          schema / "agent"            -> project.agent.asJsonLD,
          schema / "dateCreated"      -> project.dateCreated.asJsonLD,
          schema / "creator"          -> project.maybeCreator.asJsonLD,
          renku / "projectVisibility" -> project.visibility.asJsonLD,
          schema / "member"           -> project.members.toList.asJsonLD,
          schema / "schemaVersion"    -> project.version.asJsonLD,
          prov / "wasDerivedFrom"     -> project.parent.asJsonLD(encoder)
        )
      case project: Project[_] =>
        JsonLD.entity(
          project.resourceId.asEntityId,
          EntityTypes.of(prov / "Location", schema / "Project"),
          schema / "name"             -> project.name.asJsonLD,
          schema / "agent"            -> project.agent.asJsonLD,
          schema / "dateCreated"      -> project.dateCreated.asJsonLD,
          schema / "creator"          -> project.maybeCreator.asJsonLD,
          renku / "projectVisibility" -> project.visibility.asJsonLD,
          schema / "member"           -> project.members.toList.asJsonLD,
          schema / "schemaVersion"    -> project.version.asJsonLD
        )
    }
}
