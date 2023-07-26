package io.renku.projectauth

import io.renku.graph.model.{RenkuUrl, Schemas}
import io.renku.graph.model.persons.{Email, GitLabId, Name, ResourceId, Username}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

final case class ProjectMember(
    name:     Name,
    username: Username,
    gitLabId: GitLabId,
    email:    Option[Email],
    role:     Role
)

object ProjectMember {

  implicit def jsonLDEncoder(implicit renkuUrl: RenkuUrl): JsonLDEncoder[ProjectMember] =
    JsonLDEncoder.instance { member =>
      JsonLD.entity(
        ResourceId(member.gitLabId).asEntityId,
        EntityTypes.of(Schemas.schema / "Member"),
        Schemas.schema / "name" -> member.name.asJsonLD,
        Schemas.schema / "role" -> member.role.asString.asJsonLD
      )
    }
}
