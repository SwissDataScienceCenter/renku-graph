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

import Generators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.userGitLabIds
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.users.GitLabId
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.entities.EntitiesGenerators.{persons, projectEntities}
import ch.datascience.rdfstore.entities.bundles.{gitLabApiUrl, renkuBaseUrl}
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
   */

  private trait TestCase {
    val updatesCreator = new UpdatesCreator(renkuBaseUrl)
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
}
