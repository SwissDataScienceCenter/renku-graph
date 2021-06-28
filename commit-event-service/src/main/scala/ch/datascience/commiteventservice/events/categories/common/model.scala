package ch.datascience.commiteventservice.events.categories.common

import ch.datascience.graph.model.projects.{Id, Path, Visibility}

private[categories] final case class ProjectInfo(
    id:         Id,
    visibility: Visibility,
    path:       Path
)
