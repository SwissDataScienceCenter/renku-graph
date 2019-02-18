package ch.datascience.triplesgenerator.eventprocessing
import ch.datascience.graph.events.{CommitId, ProjectPath}

private sealed trait Commit {
  val id:          CommitId
  val projectPath: ProjectPath
}

private object Commit {

  final case class CommitWithParent(
      id:          CommitId,
      parentId:    CommitId,
      projectPath: ProjectPath
  ) extends Commit

  final case class CommitWithoutParent(
      id:          CommitId,
      projectPath: ProjectPath
  ) extends Commit
}
