package ch.datascience.commiteventservice.commitsync

import ch.datascience.graph.model.events.{CategoryName, CommitId}
import ch.datascience.graph.model.projects
import ch.datascience.tinytypes.{InstantTinyType, TinyTypeFactory}
import ch.datascience.tinytypes.constraints.InstantNotInTheFuture

import java.time.Instant

object model {}

// TODO: should this live in Graph Commons?
final class LastSyncedDate private (val value: Instant) extends AnyVal with InstantTinyType
object LastSyncedDate extends TinyTypeFactory[LastSyncedDate](new LastSyncedDate(_)) with InstantNotInTheFuture

final case class CommitProject(id: projects.Id, path: projects.Path)

final case class CommitSyncRequest(categoryName: CategoryName,
                                   id:           CommitId,
                                   project:      CommitProject,
                                   lastSynced:   LastSyncedDate
)
