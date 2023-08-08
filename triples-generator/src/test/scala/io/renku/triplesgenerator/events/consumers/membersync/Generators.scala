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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.positiveInts
import io.renku.graph.model.GraphModelGenerators.{personGitLabIds, personNames, personResourceIds}
import io.renku.graph.model.RenkuUrl
import io.renku.projectauth.Role
import org.scalacheck.Gen

private object Generators {

  implicit val gitLabProjectMembers: Gen[GitLabProjectMember] = for {
    id   <- personGitLabIds
    name <- personNames
  } yield GitLabProjectMember(id, name, Role.toGitLabAccessLevel(Role.Owner))

  implicit def kgProjectMembers(implicit renkuUrl: RenkuUrl): Gen[KGProjectMember] = for {
    memberId <- personResourceIds
    gitLabId <- personGitLabIds
  } yield KGProjectMember(memberId, gitLabId)

  lazy val syncSummaries: Gen[SyncSummary] =
    (positiveInts().map(_.value), positiveInts().map(_.value)).mapN(SyncSummary)
}
