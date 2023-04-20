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

package io.renku.entities.viewings.search

import io.renku.entities.search.model.{MatchingScore, Entity => SearchEntity}
import io.renku.entities.viewings.search.RecentEntitiesFinder.Criteria
import io.renku.generators.Generators.Implicits._
import io.renku.http.server.security.model.AuthUser

import java.time.Instant

class ProjectQuerySpec extends SearchTestBase {

  it should "return one project entry if viewed multiple times" in {
    val project = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val person = personGen.generateOne
    upload(projectsDataset, person)

    val userId = person.maybeGitLabId.get

    provisionTestProjects(project).unsafeRunSync()
    storeProjectViewed(userId, Instant.now().minusSeconds(60), project.path)
    storeProjectViewed(userId, Instant.now().minusSeconds(30), project.path)
    storeProjectViewed(userId, Instant.now(), project.path)

    val query = ProjectQuery.makeQuery(Criteria(Set.empty, AuthUser(userId, token), 5))

    val decoded = tsClient.queryExpecting[List[SearchEntity.Project]](query)(projectDecoder).unsafeRunSync()
    decoded.head shouldMatchTo
      SearchEntity.Project(
        matchingScore = MatchingScore(1f),
        path = project.path,
        name = project.name,
        visibility = project.visibility,
        date = project.dateCreated,
        maybeCreator = project.maybeCreator.map(_.name),
        keywords = project.keywords.toList.sorted,
        maybeDescription = project.maybeDescription,
        images = project.images
      )
  }

  it should "find and decode projects" in {
    val project = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val person = personGen.generateOne
    upload(projectsDataset, person)

    val userId = person.maybeGitLabId.get

    provisionTestProjects(project).unsafeRunSync()
    storeProjectViewed(userId, Instant.now(), project.path)

    val query = ProjectQuery.makeQuery(Criteria(Set.empty, AuthUser(userId, token), 5))

    val decoded = tsClient.queryExpecting[List[SearchEntity.Project]](query)(projectDecoder).unsafeRunSync()
    decoded.head shouldMatchTo
      SearchEntity.Project(
        matchingScore = MatchingScore(1f),
        path = project.path,
        name = project.name,
        visibility = project.visibility,
        date = project.dateCreated,
        maybeCreator = project.maybeCreator.map(_.name),
        keywords = project.keywords.toList.sorted,
        maybeDescription = project.maybeDescription,
        images = project.images
      )
  }

  it should "only return projects for the given user" in {
    val project1 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne
    val project2 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val person1 = personGen.generateOne
    val person2 = personGen.generateOne
    upload(projectsDataset, person1, person2)
    provisionTestProjects(project1, project2).unsafeRunSync()

    storeProjectViewed(person1.maybeGitLabId.get, Instant.now(), project1.path)
    storeProjectViewed(person2.maybeGitLabId.get, Instant.now(), project2.path)

    val query = ProjectQuery.makeQuery(Criteria(Set.empty, AuthUser(person1.maybeGitLabId.get, token), 5))

    val decoded = tsClient.queryExpecting[List[SearchEntity.Project]](query)(projectDecoder).unsafeRunSync()
    decoded.head shouldMatchTo
      SearchEntity.Project(
        matchingScore = MatchingScore(1f),
        path = project1.path,
        name = project1.name,
        visibility = project1.visibility,
        date = project1.dateCreated,
        maybeCreator = project1.maybeCreator.map(_.name),
        keywords = project1.keywords.toList.sorted,
        maybeDescription = project1.maybeDescription,
        images = project1.images
      )
  }
}
