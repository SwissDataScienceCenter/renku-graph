/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
