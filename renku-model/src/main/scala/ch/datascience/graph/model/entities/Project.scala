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
import ch.datascience.graph.model._
import io.renku.jsonld.JsonLDDecoder
import cats.syntax.all._

sealed trait Project {
  val resourceId:   ResourceId
  val path:         Path
  val name:         Name
  val agent:        CliVersion
  val dateCreated:  DateCreated
  val maybeCreator: Option[Person]
  val visibility:   Visibility
  val members:      Set[Person]
  val version:      SchemaVersion
}

final case class ProjectWithoutParent(resourceId:   ResourceId,
                                      path:         Path,
                                      name:         Name,
                                      agent:        CliVersion,
                                      dateCreated:  DateCreated,
                                      maybeCreator: Option[Person],
                                      visibility:   Visibility,
                                      members:      Set[Person],
                                      version:      SchemaVersion
) extends Project

final case class ProjectWithParent(resourceId:       ResourceId,
                                   path:             Path,
                                   name:             Name,
                                   agent:            CliVersion,
                                   dateCreated:      DateCreated,
                                   maybeCreator:     Option[Person],
                                   visibility:       Visibility,
                                   members:          Set[Person],
                                   version:          SchemaVersion,
                                   parentResourceId: ResourceId
) extends Project

object Project {

  import ch.datascience.graph.model.Schemas._
  import ch.datascience.graph.model.views.TinyTypeJsonLDEncoders._
  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

  private val entityTypes = EntityTypes.of(prov / "Location", schema / "Project")

  implicit def encoder[P <: Project](implicit gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[P] =
    JsonLDEncoder.instance {
      case project: ProjectWithParent =>
        JsonLD.entity(
          project.resourceId.asEntityId,
          entityTypes,
          schema / "name"             -> project.name.asJsonLD,
          schema / "agent"            -> project.agent.asJsonLD,
          schema / "dateCreated"      -> project.dateCreated.asJsonLD,
          schema / "creator"          -> project.maybeCreator.asJsonLD,
          renku / "projectVisibility" -> project.visibility.asJsonLD,
          schema / "member"           -> project.members.toList.asJsonLD,
          schema / "schemaVersion"    -> project.version.asJsonLD,
          prov / "wasDerivedFrom"     -> project.parentResourceId.asEntityId.asJsonLD
        )
      case project: Project =>
        JsonLD.entity(
          project.resourceId.asEntityId,
          entityTypes,
          schema / "name"             -> project.name.asJsonLD,
          schema / "agent"            -> project.agent.asJsonLD,
          schema / "dateCreated"      -> project.dateCreated.asJsonLD,
          schema / "creator"          -> project.maybeCreator.asJsonLD,
          renku / "projectVisibility" -> project.visibility.asJsonLD,
          schema / "member"           -> project.members.toList.asJsonLD,
          schema / "schemaVersion"    -> project.version.asJsonLD
        )
    }

  implicit def decoder(gitLabInfo: GitLabProjectInfo, potentialMembers: List[Person])(implicit
      renkuBaseUrl:                RenkuBaseUrl
  ): JsonLDDecoder[Project] = JsonLDDecoder.entity(entityTypes) { cursor =>
    import ch.datascience.graph.model.views.TinyTypeJsonLDDecoders._

    def byName(member: ProjectMember): Person => Boolean =
      person => person.name == member.name || person.name.value == member.username.value

    val maybeCreator: Option[Person] =
      gitLabInfo.maybeCreatorGitLabId
        .flatMap(creatorId => gitLabInfo.members.find(_.gitLabId == creatorId))
        .flatMap { projectMember =>
          potentialMembers.find(byName(projectMember)).map(_.copy(maybeGitLabId = Some(projectMember.gitLabId)))
        }

    val members: Set[Person] = gitLabInfo.members.map(projectMember =>
      potentialMembers
        .find(byName(projectMember))
        .map(_.copy(maybeGitLabId = Some(projectMember.gitLabId)))
        .getOrElse(
          Person(users.ResourceId((renkuBaseUrl / "persons" / projectMember.name).show),
                 projectMember.name,
                 None,
                 None,
                 projectMember.gitLabId.some
          )
        )
    )

    for {
      resourceId    <- cursor.downEntityId.as[ResourceId]
      agent         <- cursor.downField(schema / "agent").as[CliVersion]
      schemaVersion <- cursor.downField(schema / "schemaVersion").as[SchemaVersion]
    } yield gitLabInfo.maybeParentPath match {
      case Some(parentPath) =>
        ProjectWithParent(
          resourceId,
          gitLabInfo.path,
          gitLabInfo.name,
          agent,
          gitLabInfo.dateCreated,
          maybeCreator,
          gitLabInfo.visibility,
          members,
          schemaVersion,
          ResourceId(renkuBaseUrl, parentPath)
        )
      case None =>
        ProjectWithoutParent(resourceId,
                             gitLabInfo.path,
                             gitLabInfo.name,
                             agent,
                             gitLabInfo.dateCreated,
                             maybeCreator,
                             gitLabInfo.visibility,
                             members,
                             schemaVersion
        )
    }
  }

  final case class GitLabProjectInfo(name:                 Name,
                                     path:                 Path,
                                     dateCreated:          DateCreated,
                                     maybeCreatorGitLabId: Option[users.GitLabId],
                                     members:              Set[ProjectMember],
                                     visibility:           Visibility,
                                     maybeParentPath:      Option[Path]
  )
  final case class ProjectMember(name: users.Name, username: users.Username, gitLabId: users.GitLabId)
}
