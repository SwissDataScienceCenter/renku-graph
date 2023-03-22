package io.renku.entities.search.diff

import com.softwaremill.diffx.Diff
import io.renku.entities.search.model.Entity
import io.renku.graph.model.entities.DiffInstances

trait SearchDiffInstances extends DiffInstances {

  implicit val entityDatasetDiff: Diff[Entity.Dataset] =
    Diff.derived[Entity.Dataset]

  implicit val entityPersonDiff: Diff[Entity.Person] =
    Diff.derived[Entity.Person]

  implicit val entityProjectDiff: Diff[Entity.Project] =
    Diff.derived[Entity.Project]

  implicit val entityWorkflowTypeDiff: Diff[Entity.Workflow.WorkflowType] =
    Diff.diffForString.contramap(_.name)

  implicit val entityWorkflowDiff: Diff[Entity.Workflow] =
    Diff.derived[Entity.Workflow]

  implicit val searchEntityDiff: Diff[Entity] =
    Diff.derived[Entity]
}
