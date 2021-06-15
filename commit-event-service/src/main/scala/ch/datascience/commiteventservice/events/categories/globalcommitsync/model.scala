package ch.datascience.commiteventservice.events.categories.globalcommitsync

import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.LastSyncedDate

private case class GlobalCommitSyncEvent(project: Project, lastSynced: LastSyncedDate)
