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

import cats.effect.IO
import io.renku.graph.model.entities
import io.renku.entities.search.model.{MatchingScore, Entity => SearchEntity}
import io.renku.entities.viewings.search.RecentEntitiesFinder.{Criteria, EntityType}
import io.renku.generators.Generators.Implicits._
import io.renku.http.server.security.model.AuthUser

import java.time.Instant
import java.time.temporal.ChronoUnit

class RecentEntitiesFinderSpec extends SearchTestBase {
  def finder = new RecentEntitiesFinderImpl[IO](projectsDSConnectionInfo)

  it should "datasets and projects in one result" in {
    val project1 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne
    val project2 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val dataset1 = project1.to[entities.RenkuProject.WithoutParent].datasets.head
    val dataset2 = project2.to[entities.RenkuProject.WithoutParent].datasets.head

    val person = personGen.generateOne

    upload(projectsDataset, person)
    provisionTestProjects(project1, project2).unsafeRunSync()

    storeProjectViewed(person.maybeGitLabId.get, Instant.now().minus(1, ChronoUnit.DAYS), project1.path)
    storeDatasetViewed(person.maybeGitLabId.get,
                       Instant.now().minus(2, ChronoUnit.DAYS),
                       dataset1.identification.identifier
    )
    storeProjectViewed(person.maybeGitLabId.get, Instant.now().minus(3, ChronoUnit.DAYS), project2.path)
    storeDatasetViewed(person.maybeGitLabId.get,
                       Instant.now().minus(4, ChronoUnit.DAYS),
                       dataset2.identification.identifier
    )

    val criteria = Criteria(Set(EntityType.Project, EntityType.Dataset), AuthUser(person.maybeGitLabId.get, token), 5)
    val result   = finder.findRecentlyViewedEntities(criteria).unsafeRunSync()

    val expect = List(
      EntityType.Project -> project1.path.value,
      EntityType.Dataset -> Right(dataset1.provenance.topmostSameAs.value),
      EntityType.Project -> project2.path.value,
      EntityType.Dataset -> Right(dataset2.provenance.topmostSameAs.value)
    )

    result.pagingInfo.total.value shouldBe 4

    result.results
      .collect {
        case e: SearchEntity.Dataset => EntityType.Dataset -> e.sameAs.map(_.value)
        case e: SearchEntity.Project => EntityType.Project -> e.path.value
      } shouldBe expect
  }

  it should "return projects in most recently viewed order" in {
    val project1 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne
    val project2 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val person = personGen.generateOne

    upload(projectsDataset, person)
    provisionTestProjects(project1, project2).unsafeRunSync()

    storeProjectViewed(person.maybeGitLabId.get, Instant.now().minus(1, ChronoUnit.DAYS), project1.path)
    storeProjectViewed(person.maybeGitLabId.get, Instant.now(), project2.path)

    val criteria = Criteria(Set(EntityType.Project), AuthUser(person.maybeGitLabId.get, token), 5)
    val result   = finder.findRecentlyViewedEntities(criteria).unsafeRunSync()

    result.pagingInfo.total.value shouldBe 2
    result.results.head shouldMatchTo
      SearchEntity.Project(
        matchingScore = MatchingScore(1f),
        path = project2.path,
        name = project2.name,
        visibility = project2.visibility,
        date = project2.dateCreated,
        dateModified = project2.dateModified,
        maybeCreator = project2.maybeCreator.map(_.name),
        keywords = project2.keywords.toList.sorted,
        maybeDescription = project2.maybeDescription,
        images = project2.images
      )
  }

  it should "return apply limit when returning projects" in {
    val project1 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne
    val project2 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val person = personGen.generateOne

    upload(projectsDataset, person)
    provisionTestProjects(project1, project2).unsafeRunSync()

    storeProjectViewed(person.maybeGitLabId.get, Instant.now().minus(1, ChronoUnit.DAYS), project1.path)
    storeProjectViewed(person.maybeGitLabId.get, Instant.now(), project2.path)

    val criteria = Criteria(Set(EntityType.Project), AuthUser(person.maybeGitLabId.get, token), 1)
    val result   = finder.findRecentlyViewedEntities(criteria).unsafeRunSync()

    result.results.size         shouldBe 1
    result.pagingInfo.total.value should (be >= 1)
    result.results.head shouldMatchTo
      SearchEntity.Project(
        matchingScore = MatchingScore(1f),
        path = project2.path,
        name = project2.name,
        visibility = project2.visibility,
        date = project2.dateCreated,
        dateModified = project2.dateModified,
        maybeCreator = project2.maybeCreator.map(_.name),
        keywords = project2.keywords.toList.sorted,
        maybeDescription = project2.maybeDescription,
        images = project2.images
      )
  }
}
