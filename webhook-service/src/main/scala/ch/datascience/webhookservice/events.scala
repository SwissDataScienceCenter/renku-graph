package ch.datascience.webhookservice

import ch.datascience.tinytypes.TinyType

case class PushEvent(before: CommitBefore,
                     after: CommitAfter,
                     projectId: ProjectId)

case class CommitBefore(value: String) extends GitSha

case class CommitAfter(value: String) extends GitSha

case class ProjectId(value: Long) extends TinyType[Long]