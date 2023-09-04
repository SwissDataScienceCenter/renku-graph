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

import Generators._
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.persons.ResourceId
import io.renku.graph.model.{RenkuUrl, persons, projects}
import io.renku.projectauth.ProjectAuthData
import io.renku.triplesstore.TSClient
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.util.Try

class KGSynchronizerSpec extends AnyFlatSpec with MockFactory with should.Matchers with TryValues {

  it should "pull members from the TS, " +
    "calculate the diff and " +
    "applies the diff to the TS" in {

      val projectSlug = projectSlugs.generateOne

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

      val visibility = projectVisibilities.generateOne
      (projectAuthSync.syncProject _)
        .expects(ProjectAuthData(projectSlug, membersInGitLab.map(_.toProjectAuthMember), visibility))
        .returning(().pure[Try])

      synchronizer.syncMembers(projectSlug, membersInGitLab, visibility.some).success.value shouldBe
        SyncSummary(missingMembersWithIds.size, membersToRemove.size)
    }

  it should "remove the project auth data from the TS when no visibility is given" in {

    val projectSlug = projectSlugs.generateOne

    (kgProjectMembersFinder.findProjectMembers _)
      .expects(projectSlug)
      .returning(Set.empty[KGProjectMember].pure[Try])

    (kgPersonFinder.findPersonIds _)
      .expects(Set.empty[GitLabProjectMember])
      .returning(Set.empty[(GitLabProjectMember, Option[ResourceId])].pure[Try])

    (updatesCreator.insertion _)
      .expects(projectSlug, Set.empty[(GitLabProjectMember, Option[persons.ResourceId])])
      .returning(Nil)

    (updatesCreator.removal _)
      .expects(projectSlug, Set.empty[KGProjectMember])
      .returning(Nil)

    (projectAuthSync.removeAuthData _)
      .expects(projectSlug)
      .returning(().pure[Try])

    synchronizer.syncMembers(projectSlug, Set.empty, maybeVisibility = None).success.value shouldBe SyncSummary(0, 0)
  }

  it should "fail if collaborator fails" in {

    val projectSlug = projectSlugs.generateOne

    val exception = exceptions.generateOne
    (kgProjectMembersFinder.findProjectMembers _)
      .expects(projectSlug)
      .returning(exception.raiseError[Try, Set[KGProjectMember]])

    val visibility = projectVisibilities.generateSome

    synchronizer
      .syncMembers(projectSlug, gitLabProjectMembers.generateSet(), visibility)
      .failure
      .exception shouldBe exception
  }

  private implicit lazy val renkuUrl: RenkuUrl = renkuUrls.generateOne

  private lazy val kgProjectMembersFinder = mock[KGProjectMembersFinder[Try]]
  private lazy val kgPersonFinder         = mock[KGPersonFinder[Try]]
  private lazy val updatesCreator         = mock[UpdatesCreator]
  private lazy val tsClient               = mock[TSClient[Try]]
  private lazy val projectAuthSync        = mock[ProjectAuthSync[Try]]
  private lazy val synchronizer =
    new KGSynchronizerImpl[Try](kgProjectMembersFinder, kgPersonFinder, updatesCreator, projectAuthSync, tsClient)
}
