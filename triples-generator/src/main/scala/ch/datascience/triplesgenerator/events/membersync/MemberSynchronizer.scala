package ch.datascience.triplesgenerator.events.membersync

import ch.datascience.graph.model.projects

private trait MemberSynchronizer[Interpretation[_]] {
  def synchronizeMembers(projectPath: projects.Path): Interpretation[Unit]
}

private class MemberSynchronizerImpl[Interpretation[_]]() extends MemberSynchronizer[Interpretation] {
  override def synchronizeMembers(projectPath: projects.Path): Interpretation[Unit] = ???
}
