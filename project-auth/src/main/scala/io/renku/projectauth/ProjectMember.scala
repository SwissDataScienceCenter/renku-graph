package io.renku.projectauth

import io.renku.graph.model.persons.{GitLabId, Name, ResourceId}
import io.renku.graph.model.{RenkuUrl, Schemas}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

final case class ProjectMember(
    name:     Name,
    gitLabId: GitLabId,
    role:     Role
)

object ProjectMember {

  def fromGitLabData(gitLabId: GitLabId, name: Name, accessLevel: Int): ProjectMember =
    ProjectMember(name, gitLabId, Role.fromGitLabAccessLevel(accessLevel))

  implicit def jsonLDEncoder(implicit renkuUrl: RenkuUrl): JsonLDEncoder[ProjectMember] =
    JsonLDEncoder.instance { member =>
      JsonLD.entity(
        ResourceId(member.gitLabId).asEntityId,
        EntityTypes.of(Schemas.schema / "Member"),
        Schemas.schema / "identifier" -> member.gitLabId.asJsonLD,
        Schemas.schema / "name"       -> member.name.asJsonLD,
        Schemas.schema / "role"       -> member.role.asString.asJsonLD
      )
    }
}
