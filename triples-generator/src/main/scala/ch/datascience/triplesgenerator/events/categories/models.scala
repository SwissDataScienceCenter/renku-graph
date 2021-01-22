package ch.datascience.triplesgenerator.events.categories

import ch.datascience.graph.model.projects

object models {
  final case class Project(id: projects.Id, path: projects.Path)
}
