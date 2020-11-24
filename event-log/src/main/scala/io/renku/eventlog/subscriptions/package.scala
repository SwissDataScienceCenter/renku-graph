package io.renku.eventlog

import ch.datascience.graph.model.projects

package object subscriptions {
  final case class ProjectIds(id: projects.Id, path: projects.Path)
}
