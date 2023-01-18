package io.renku.cli.model

import io.renku.graph.model.images.Image
import io.renku.graph.model.projects._

final case class CliProject(
    id:          ResourceId,
    path:        Path,
    name:        Name,
    description: Option[Description],
    dateCreated: DateCreated,
    creator:     Option[CliPerson],
    visibility:  Visibility,
    keywords:    Set[Keyword],
    members:     Set[CliPerson],
    images:      List[Image]
)

object CliProject {}
