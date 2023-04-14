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
import io.renku.graph.model.entities
import io.renku.http.client.{AccessToken, UserAccessToken}
import io.renku.http.server.security.model.AuthUser

import java.time.Instant

class ProjectQuerySpec extends SearchTestBase {
  val token: UserAccessToken = AccessToken.PersonalAccessToken("nonblank")

  it should "find and decode projects" in {
    val project = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne
    val entitiesProject = project.to[entities.RenkuProject.WithoutParent]

    val person = personGen.generateOne
    upload(projectsDataset, person)

    val userId = person.maybeGitLabId.get

    provisionTestProjects(project).unsafeRunSync()
    storeProjectViewed(userId, Instant.now(), entitiesProject.path)
    storeDatasetViewed(userId, Instant.now(), entitiesProject.datasets.head.identification.identifier)

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
        keywords = project.keywords.toList,
        maybeDescription = project.maybeDescription,
        images = project.images
      )
  }
}
