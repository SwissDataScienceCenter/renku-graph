package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration

import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects

private[globalcommitsync] final case class CommitWithParents(id:        CommitId,
                                                             projectId: projects.Id,
                                                             parents:   List[CommitId]
)
