/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import io.renku.entities.viewings.ViewingsCollectorJenaSpec
import io.renku.entities.viewings.search.RecentEntitiesFinder.Criteria
import io.renku.generators.Generators.Implicits._
import io.renku.http.server.security.model.AuthUser
import org.scalatest.Succeeded

import java.time.Instant

class ProjectQuerySpec extends SearchSpec with ViewingsCollectorJenaSpec {

  it should "return one project entry if viewed multiple times" in projectsDSConfig.use { implicit pcc =>
    val project = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .generateOne

    val person = personGen.generateOne
    for {
      _ <- uploadToProjects(person)

      userId = person.maybeGitLabId.get

      _ <- provisionTestProjects(project)
      _ <- storeProjectViewed(userId, Instant.now().minusSeconds(60), project.slug)
      _ <- storeProjectViewed(userId, Instant.now().minusSeconds(30), project.slug)
      _ <- storeProjectViewed(userId, Instant.now(), project.slug)

      query = ProjectQuery.makeQuery(Criteria(Set.empty, AuthUser(userId, token), 5))

      _ <- tsClient.queryExpecting[List[SearchEntity.Project]](query)(projectDecoder).asserting {
             _.head shouldMatchTo
               SearchEntity.Project(
                 matchingScore = MatchingScore(1f),
                 slug = project.slug,
                 name = project.name,
                 visibility = project.visibility,
                 date = project.dateCreated,
                 dateModified = project.dateModified,
                 maybeCreator = project.maybeCreator.map(_.name),
                 keywords = project.keywords.toList.sorted,
                 maybeDescription = project.maybeDescription,
                 images = project.images
               )
           }
    } yield Succeeded
  }

  it should "find and decode projects" in projectsDSConfig.use { implicit pcc =>
    val project = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .generateOne

    val person = personGen.generateOne
    for {
      _ <- uploadToProjects(person)

      userId = person.maybeGitLabId.get

      _ <- provisionTestProjects(project)
      _ <- storeProjectViewed(userId, Instant.now(), project.slug)

      query = ProjectQuery.makeQuery(Criteria(Set.empty, AuthUser(userId, token), 5))

      _ <- tsClient.queryExpecting[List[SearchEntity.Project]](query)(projectDecoder).asserting {
             _.head shouldMatchTo
               SearchEntity.Project(
                 matchingScore = MatchingScore(1f),
                 slug = project.slug,
                 name = project.name,
                 visibility = project.visibility,
                 date = project.dateCreated,
                 dateModified = project.dateModified,
                 maybeCreator = project.maybeCreator.map(_.name),
                 keywords = project.keywords.toList.sorted,
                 maybeDescription = project.maybeDescription,
                 images = project.images
               )
           }
    } yield Succeeded
  }

  it should "only return projects for the given user" in projectsDSConfig.use { implicit pcc =>
    val project1 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .generateOne
    val project2 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .generateOne

    val person1 = personGen.generateOne
    val person2 = personGen.generateOne
    for {
      _ <- uploadToProjects(person1, person2)
      _ <- provisionTestProjects(project1, project2)

      _ <- storeProjectViewed(person1.maybeGitLabId.get, Instant.now(), project1.slug)
      _ <- storeProjectViewed(person2.maybeGitLabId.get, Instant.now(), project2.slug)

      query = ProjectQuery.makeQuery(Criteria(Set.empty, AuthUser(person1.maybeGitLabId.get, token), 5))

      _ <- tsClient.queryExpecting[List[SearchEntity.Project]](query)(projectDecoder).asserting {
             _.head shouldMatchTo
               SearchEntity.Project(
                 matchingScore = MatchingScore(1f),
                 slug = project1.slug,
                 name = project1.name,
                 visibility = project1.visibility,
                 date = project1.dateCreated,
                 dateModified = project1.dateModified,
                 maybeCreator = project1.maybeCreator.map(_.name),
                 keywords = project1.keywords.toList.sorted,
                 maybeDescription = project1.maybeDescription,
                 images = project1.images
               )
           }
    } yield Succeeded
  }
}
