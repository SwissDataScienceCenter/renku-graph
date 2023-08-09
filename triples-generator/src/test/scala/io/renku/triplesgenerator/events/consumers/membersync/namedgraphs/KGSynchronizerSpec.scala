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
package namedgraphs

import Generators._
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.{RenkuUrl, projects}
import io.renku.triplesgenerator.events.consumers.ProjectAuthSync
import io.renku.triplesgenerator.gitlab.GitLabProjectMember
import io.renku.triplesgenerator.gitlab.Generators._
import io.renku.triplesstore.TSClient
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class KGSynchronizerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "pulls members from KG, " +
    "calculate the diff and " +
    "applies it in the TS" in new TestCase {

      val gitLabMemberMissingInKG = gitLabProjectMembers.generateOne
      val gitLabMemberAlsoInKG    = gitLabProjectMembers.generateOne
      val kgMemberAlsoInGitLab    = kgProjectMembers.generateOne.copy(gitLabId = gitLabMemberAlsoInKG.gitLabId)
      val kgMemberMissingInGitLab = kgProjectMembers.generateOne

      val membersInGitLab = Set(gitLabMemberMissingInKG, gitLabMemberAlsoInKG)
      val membersInKG     = Set(kgMemberAlsoInGitLab, kgMemberMissingInGitLab)

      (kgProjectMembersFinder
        .findProjectMembers(_: projects.Slug))
        .expects(projectSlug)
        .returning(membersInKG.pure[Try])

      val missingMembersWithIds = Set(gitLabMemberMissingInKG -> personResourceIds.generateOption)
      (kgPersonFinder
        .findPersonIds(_: Set[GitLabProjectMember]))
        .expects(Set(gitLabMemberMissingInKG))
        .returning(missingMembersWithIds.pure[Try])

      val insertionQueries = sparqlQueries.generateNonEmptyList().toList
      (updatesCreator.insertion _)
        .expects(projectSlug, missingMembersWithIds)
        .returning(insertionQueries)

      val removalQueries  = sparqlQueries.generateNonEmptyList().toList
      val membersToRemove = Set(kgMemberMissingInGitLab)
      (updatesCreator.removal _)
        .expects(projectSlug, membersToRemove)
        .returning(removalQueries)

      (removalQueries ::: insertionQueries) foreach { query =>
        (tsClient.updateWithNoResult _)
          .expects(query)
          .returning(().pure[Try])
      }

      synchronizer.syncMembers(projectSlug, membersInGitLab) shouldBe
        SyncSummary(missingMembersWithIds.size, membersToRemove.size).pure[Try]
    }

  "fail if collaborator fails" in new TestCase {

    val exception = exceptions.generateOne
    (kgProjectMembersFinder.findProjectMembers _)
      .expects(projectSlug)
      .returning(exception.raiseError[Try, Set[KGProjectMember]])

    synchronizer.syncMembers(projectSlug, gitLabProjectMembers.generateSet()) shouldBe exception
      .raiseError[Try, Set[KGProjectMember]]
  }

  private trait TestCase {
    implicit val renkuUrl: RenkuUrl = renkuUrls.generateOne
    val projectSlug = projectSlugs.generateOne

    val kgProjectMembersFinder = mock[KGProjectMembersFinder[Try]]
    val kgPersonFinder         = mock[KGPersonFinder[Try]]
    val updatesCreator         = mock[UpdatesCreator]
    val tsClient               = mock[TSClient[Try]]
    val projectAuthSync        = mock[ProjectAuthSync[Try]]

    val synchronizer =
      new KGSynchronizerImpl[Try](kgProjectMembersFinder, kgPersonFinder, updatesCreator, projectAuthSync, tsClient)
  }
}
