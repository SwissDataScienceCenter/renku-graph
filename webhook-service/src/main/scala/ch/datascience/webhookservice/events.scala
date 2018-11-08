package ch.datascience.webhookservice

case class PushEvent(before: CommitBefore,
                     after: CommitAfter,
                     projectId: ProjectId)

case class CommitBefore(value: String) {
  override val toString: String = value
}

case class CommitAfter(value: String) {
  override val toString: String = value
}

case class ProjectId(value: Long) {
  override val toString: String = value.toString
}