package io.renku.projectauth

import io.renku.graph.model.{GitLabUrl, RenkuUrl, Schemas, persons}
import io.renku.graph.model.projects.{GitLabId, Path, ResourceId, Visibility}
import io.renku.jsonld.JsonLD.JsonLDArray
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

final case class ProjectAuthData(
    id:         GitLabId,
    path:       Path,
    members:    Set[ProjectMember],
    visibility: Visibility
)

object ProjectAuthData {

  implicit def jsonLDEncoder(implicit renkuUrl: RenkuUrl): JsonLDEncoder[ProjectAuthData] =
    JsonLDEncoder.instance { data =>
      JsonLD.entity(
        ResourceId(data.path).asEntityId,
        EntityTypes.of(Schemas.schema / "Project"),
        Schemas.renku / "projectId"  -> data.id.asJsonLD,
        Schemas.renku / "slug"       -> data.path.asJsonLD,
        Schemas.renku / "visibility" -> data.visibility.asJsonLD,
        Schemas.renku / "members" -> JsonLDArray(
          data.members.toSeq.map(_.asJsonLD)
        )
      )
    }
}
