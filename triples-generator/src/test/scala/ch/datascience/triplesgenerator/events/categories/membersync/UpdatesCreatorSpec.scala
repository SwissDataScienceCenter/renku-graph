/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.membersync

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.{projectPaths, userEmails, userGitLabIds}
import ch.datascience.graph.model.users.{Email, GitLabId}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.graph.model.{projects, users}
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.entities.EntitiesGenerators.{persons, projectEntities}
import ch.datascience.rdfstore.entities.bundles.{gitLabApiUrl, renkuBaseUrl}
import ch.datascience.triplesgenerator.events.categories.membersync.Generators._
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class UpdatesCreatorSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  "removal" should {

    "prepare query to delete the member links to project" in new TestCase {
      val memberToRemove0 = persons(userGitLabIds.toGeneratorOfSomes).generateOne
      val memberToRemove1 = persons(userGitLabIds.toGeneratorOfSomes).generateOne
      val memberToStay    = persons(userGitLabIds.toGeneratorOfSomes).generateOne
      val allMembers      = Set(memberToRemove0, memberToRemove1, memberToStay)
      val project         = projectEntities.generateOne.copy(members = allMembers)

      loadToStore(project.asJsonLD)

      findMembers(project.path) shouldBe allMembers.flatMap(_.maybeGitLabId)

      val query = updatesCreator.removal(
        project.path,
        Set(memberToRemove0, memberToRemove1).toKGProjectMembers + kgProjectMembers.generateOne
      )

      runUpdate(query).unsafeRunSync()

      findMembers(project.path) shouldBe Set(memberToStay.maybeGitLabId).flatten
    }
  }

  "insertion" should {

    "prepare queries to insert links for members existing in KG" in new TestCase {
      val member = gitLabProjectMembers.generateOne
      val personInKG = persons(maybeEmails = userEmails.toGeneratorOfSomes).generateOne.copy(
        maybeGitLabId = Some(member.gitLabId)
      )
      val personJsonLD = personInKG.asJsonLD

      val project = projectEntities.generateOne.copy(members = Set.empty)

      loadToStore(project.asJsonLD, personJsonLD)

      findMembers(project.path) shouldBe Set.empty

      val queries = updatesCreator.insertion(
        project.path,
        Set(member -> personJsonLD.entityId.map(users.ResourceId.apply))
      )

      queries.map(runUpdate).sequence.unsafeRunSync()

      findMembersEmails(project.path) shouldBe Set(
        member.gitLabId -> personInKG.maybeEmail
      )
    }

    "prepare queries to insert links and new person for members non-existing in KG" in new TestCase {
      val member = gitLabProjectMembers.generateOne

      val project = projectEntities.generateOne.copy(members = Set.empty)

      loadToStore(project.asJsonLD)

      findMembers(project.path) shouldBe Set.empty

      val queries = updatesCreator.insertion(
        project.path,
        Set(member -> Option.empty[users.ResourceId])
      )

      queries.map(runUpdate).sequence.unsafeRunSync()

      findMembersEmails(project.path) shouldBe Set(
        member.gitLabId -> Option.empty[users.Email]
      )
    }

    "prepare queries to insert the project and then the members when neither exists in KG" in new TestCase {
      val member = gitLabProjectMembers.generateOne

      val projectPath = projectPaths.generateOne

      findMembers(projectPath) shouldBe Set.empty

      val queries = updatesCreator.insertion(
        projectPath,
        Set(member -> Option.empty[users.ResourceId])
      )

      queries.map(runUpdate).sequence.unsafeRunSync()

      findMembersEmails(projectPath) shouldBe Set(
        member.gitLabId -> Option.empty[users.Email]
      )
    }

    "prepare queries to insert a project and attach a member already in KG when the project didn't previously exist" in new TestCase {
      val member = gitLabProjectMembers.generateOne

      val projectPath = projectPaths.generateOne
      val personInKG = persons(maybeEmails = userEmails.toGeneratorOfSomes).generateOne.copy(
        maybeGitLabId = Some(member.gitLabId)
      )
      val personJsonLD = personInKG.asJsonLD
      loadToStore(personJsonLD)

      findMembers(projectPath) shouldBe Set.empty

      val queries = updatesCreator.insertion(
        projectPath,
        Set(member -> personJsonLD.entityId.map(users.ResourceId.apply))
      )

      queries.map(runUpdate).sequence.unsafeRunSync()

      findMembersEmails(projectPath) shouldBe Set(
        member.gitLabId -> personInKG.maybeEmail
      )
    }
  }

  /*

  Difficult situation would be if the person doesn't exist in KG yet but we need to create the link to the project
  We can fetch this information when we're fetching project members

  if there are no members of this project in KG

  go through each of the GitLab members and find
  if there's a person with this GitLab ID in KG but not linked in the project
    link them to project
  else
    create a new person + the link

  for all existing members in KG who have been removed in gitlab,
    remove link in KG


    - new project created and initial push to gitlab
    - Gitlab sends commit event to EL
    - EL puts a new event to the event table and new project to the project table
    - EL will trigger two events
      - commit event
      - members sync
    - both events reach TG at the same time
    - member sync event is first
      - links are created regardless of the project exists in KG
   */

  private trait TestCase {
    val updatesCreator = new UpdatesCreator(renkuBaseUrl, gitLabApiUrl)
  }

  private def findMembers(path: projects.Path): Set[GitLabId] =
    runQuery(
      s"""|SELECT DISTINCT ?gitLabId
          |WHERE {
          |  ${projects.ResourceId(renkuBaseUrl, path).showAs[RdfResource]} schema:member ?memberId.
          |                                                        
          |  ?memberId  rdf:type      schema:Person;
          |             schema:sameAs ?sameAsId. 
          |             
          |  ?sameAsId  schema:additionalType  'GitLab';
          |             schema:identifier      ?gitLabId.
          |}
          |""".stripMargin
    )
      .unsafeRunSync()
      .flatMap(row => row.get("gitLabId").map(_.toInt).map(GitLabId.apply))
      .toSet

  private def findMembersEmails(path: projects.Path): Set[(GitLabId, Option[Email])] =
    runQuery(
      s"""|SELECT DISTINCT ?gitLabId ?email
          |WHERE {
          |  ${projects.ResourceId(renkuBaseUrl, path).showAs[RdfResource]} schema:member ?memberId.
          |  ?memberId  rdf:type     schema:Person;
          |             schema:sameAs ?sameAsId. 
          |             
          |  ?sameAsId  schema:additionalType  'GitLab';
          |             schema:identifier      ?gitLabId. 
          |             
          |  OPTIONAL {
          |    ?memberId  schema:email ?email. 
          |  }
          |}
          |""".stripMargin
    )
      .unsafeRunSync()
      .map(row => GitLabId(row("gitLabId").toInt) -> row.get("email").map(Email.apply))
      .toSet
}
