/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing

import ch.datascience.graph.model.EventsGenerators.{commitIds, projects}
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import org.scalacheck.Gen

private object EventProcessingGenerators {

  implicit val commits: Gen[Commit] = for {
    commitId      <- commitIds
    project       <- projects
    maybeParentId <- Gen.option(commitIds)
  } yield maybeParentId match {
    case None           => CommitWithoutParent(commitId, project)
    case Some(parentId) => CommitWithParent(commitId, parentId, project)
  }
}
