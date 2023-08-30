package io.renku.projectauth.util

import fs2.Pipe
import io.circe.Decoder
import io.renku.graph.model.projects.{Slug, Visibility}
import io.renku.projectauth.{ProjectAuthData, ProjectMember}
import io.renku.triplesstore.client.http.RowDecoder
import io.renku.tinytypes.json.TinyTypeDecoders._

final case class ProjectAuthDataRow(slug: Slug, visibility: Visibility, memberRole: Option[ProjectMember])

object ProjectAuthDataRow {

  private implicit val projectMemberDecoder: Decoder[ProjectMember] =
    Decoder.decodeString.emap(ProjectMember.fromEncoded)

  implicit val tupleRowDecoder: RowDecoder[ProjectAuthDataRow] =
    RowDecoder.forProduct3("slug", "visibility", "memberRole")(ProjectAuthDataRow.apply)

  def collect[F[_]]: Pipe[F, ProjectAuthDataRow, ProjectAuthData] =
    _.groupAdjacentBy(_.slug)
      .map { case (slug, rest) =>
        val members = rest.toList.flatMap(_.memberRole)
        val vis     = rest.head.map(_.visibility)
        vis.map(v => ProjectAuthData(slug, members.toSet, v))
      }
      .unNone
}
