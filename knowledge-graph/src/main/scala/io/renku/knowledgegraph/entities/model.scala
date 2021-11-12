package io.renku.knowledgegraph.entities

import io.renku.graph.model.projects

object model {

  sealed trait Entity

  final case class Project(
      name:         projects.Name,
      path:         projects.Path,
      visibility:   projects.Visibility,
      dateCreated:  projects.DateCreated,
      maybeCreator: Option[projects.DateCreated],
      description:  projects.Description
  ) extends Entity
}
