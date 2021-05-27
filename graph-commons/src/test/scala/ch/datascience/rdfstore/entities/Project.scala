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
import ch.datascience.rdfstore.entities.Project.ForksCount
import ch.datascience.tinytypes.constraints.PositiveInt
import ch.datascience.tinytypes.{IntTinyType, TinyTypeFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive

case class Project[+FC <: ForksCount](path:         Path,
                                      name:         Name,
                                      agent:        CliVersion,
                                      dateCreated:  DateCreated,
                                      maybeCreator: Option[Person],
                                      visibility:   Visibility,
                                      forksCount:   FC,
                                      members:      Set[Person],
                                      version:      SchemaVersion
)

trait HavingParent {
  val parent: Project[ForksCount.NonZero]
}

object Project {

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import JsonLDEncoder._
  import io.renku.jsonld.syntax._

  def withoutParent[FC <: ForksCount](path:         Path,
                                      name:         Name,
                                      agent:        CliVersion,
                                      dateCreated:  DateCreated,
                                      maybeCreator: Option[Person],
                                      visibility:   Visibility,
                                      forksCount:   FC,
                                      members:      Set[Person],
                                      version:      SchemaVersion
  ): Project[FC] = Project[FC](path, name, agent, dateCreated, maybeCreator, visibility, forksCount, members, version)

  def withParent[FC <: ForksCount](path:          Path,
                                   name:          Name,
                                   agent:         CliVersion,
                                   dateCreated:   DateCreated,
                                   maybeCreator:  Option[Person],
                                   visibility:    Visibility,
                                   forksCount:    FC,
                                   members:       Set[Person],
                                   version:       SchemaVersion,
                                   parentProject: Project[ForksCount.NonZero]
  ): Project[FC] with HavingParent =
    new Project[FC](path, name, agent, dateCreated, maybeCreator, visibility, forksCount, members, version)
      with HavingParent { override val parent = parentProject }

  sealed trait ForksCount extends Any with IntTinyType

  object ForksCount {

    def apply(count: Int Refined Positive): NonZero = NonZero(count.value)

    case object Zero extends ForksCount { override val value: Int = 0 }
    type Zero = Zero.type

    final class NonZero private (val value: Int) extends AnyVal with ForksCount
    object NonZero extends TinyTypeFactory[NonZero](new NonZero(_)) with PositiveInt
  }

  implicit def encoder[P <: Project[_]](implicit
      renkuBaseUrl: RenkuBaseUrl,
      gitLabApiUrl: GitLabApiUrl
  ): JsonLDEncoder[P] = JsonLDEncoder.instance {
    case project: Project[_] with HavingParent =>
      JsonLD.entity(
        project.asEntityId,
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
        project.asEntityId,
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

  implicit def entityIdEncoder[P <: Project[_]](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[P] =
    EntityIdEncoder.instance(project => renkuBaseUrl / "projects" / project.path)

  private implicit val projectResourceToEntityId: ResourceId => EntityId =
    resource => EntityId of resource.value
}
