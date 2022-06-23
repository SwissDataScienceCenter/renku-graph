package io.renku.eventlog.events.categories.cleanuprequest

import cats.Show
import cats.syntax.all._
import io.renku.graph.model.projects

private trait CleanUpRequestEvent extends Product with Serializable

private object CleanUpRequestEvent {
  def apply(projectId: projects.Id, projectPath: projects.Path): CleanUpRequestEvent =
    CleanUpRequestEvent.Full(projectId, projectPath)
  def apply(projectPath: projects.Path): CleanUpRequestEvent =
    CleanUpRequestEvent.Partial(projectPath)

  final case class Full(projectId: projects.Id, projectPath: projects.Path) extends CleanUpRequestEvent
  final case class Partial(projectPath: projects.Path)                      extends CleanUpRequestEvent

  implicit val show: Show[CleanUpRequestEvent] = Show.show {
    case e: Full    => e.show
    case e: Partial => e.show
  }

  implicit val showFull: Show[Full] = Show.show { case Full(id, path) =>
    show"projectId = $id, projectPath = $path"
  }
  implicit val showPartial: Show[Partial] = Show.show { case Partial(path) => show"projectPath = $path" }
}
