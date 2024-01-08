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

import cats.effect.IO
import io.renku.entities.search.model.{MatchingScore, Entity => SearchEntity}
import io.renku.entities.viewings.ViewingsCollectorJenaSpec
import io.renku.entities.viewings.search.RecentEntitiesFinder.{Criteria, EntityType}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.ProjectsConnectionConfig
import org.scalatest.Succeeded

import java.time.Instant
import java.time.temporal.ChronoUnit

class RecentEntitiesFinderSpec extends SearchSpec with ViewingsCollectorJenaSpec {

  private def recentEntitiesFinder(implicit pcc: ProjectsConnectionConfig) =
    new RecentEntitiesFinderImpl[IO](tsClient)

  it should "datasets and projects in one result" in projectsDSConfig.use { implicit pcc =>
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

    for {
      _ <- uploadToProjects(person.to[entities.Person])
      _ <- provisionTestProjects(project1, project2)

      _ <- storeProjectViewed(person.maybeGitLabId.get, Instant.now().minus(1, ChronoUnit.DAYS), project1.slug)
      _ <- storeDatasetViewed(person.maybeGitLabId.get,
                              Instant.now().minus(2, ChronoUnit.DAYS),
                              dataset1.identification.identifier
           )
      _ <- storeProjectViewed(person.maybeGitLabId.get, Instant.now().minus(3, ChronoUnit.DAYS), project2.slug)
      _ <- storeDatasetViewed(person.maybeGitLabId.get,
                              Instant.now().minus(4, ChronoUnit.DAYS),
                              dataset2.identification.identifier
           )

      criteria = Criteria(Set(EntityType.Project, EntityType.Dataset), AuthUser(person.maybeGitLabId.get, token), 5)
      result <- recentEntitiesFinder.findRecentlyViewedEntities(criteria)

      _ = result.pagingInfo.total.value shouldBe 4

      expect = List(
                 EntityType.Project -> project1.slug.value,
                 EntityType.Dataset -> dataset1.provenance.topmostSameAs.value,
                 EntityType.Project -> project2.slug.value,
                 EntityType.Dataset -> dataset2.provenance.topmostSameAs.value
               )
      _ = result.results
            .collect {
              case e: SearchEntity.Dataset => EntityType.Dataset -> e.sameAs.value
              case e: SearchEntity.Project => EntityType.Project -> e.slug.value
            } shouldBe expect
    } yield Succeeded
  }

  it should "return projects in most recently viewed order" in projectsDSConfig.use { implicit pcc =>
    val project1 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne
    val project2 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val person = personGen.generateOne

    for {
      _ <- uploadToProjects(person)
      _ <- provisionTestProjects(project1, project2)

      _ <- storeProjectViewed(person.maybeGitLabId.get, Instant.now().minus(1, ChronoUnit.DAYS), project1.slug)
      _ <- storeProjectViewed(person.maybeGitLabId.get, Instant.now(), project2.slug)

      criteria = Criteria(Set(EntityType.Project), AuthUser(person.maybeGitLabId.get, token), 5)
      result <- recentEntitiesFinder.findRecentlyViewedEntities(criteria)

      _ = result.pagingInfo.total.value shouldBe 2
      _ = result.results.head shouldMatchTo
            SearchEntity.Project(
              matchingScore = MatchingScore(1f),
              slug = project2.slug,
              name = project2.name,
              visibility = project2.visibility,
              date = project2.dateCreated,
              dateModified = project2.dateModified,
              maybeCreator = project2.maybeCreator.map(_.name),
              keywords = project2.keywords.toList.sorted,
              maybeDescription = project2.maybeDescription,
              images = project2.images
            )
    } yield Succeeded
  }

  it should "return apply limit when returning projects" in projectsDSConfig.use { implicit pcc =>
    val project1 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne
    val project2 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val person = personGen.generateOne

    for {
      _ <- uploadToProjects(person)
      _ <- provisionTestProjects(project1, project2)

      _ <- storeProjectViewed(person.maybeGitLabId.get, Instant.now().minus(1, ChronoUnit.DAYS), project1.slug)
      _ <- storeProjectViewed(person.maybeGitLabId.get, Instant.now(), project2.slug)

      criteria = Criteria(Set(EntityType.Project), AuthUser(person.maybeGitLabId.get, token), 1)
      result <- recentEntitiesFinder.findRecentlyViewedEntities(criteria)

      _ = result.results.size         shouldBe 1
      _ = result.pagingInfo.total.value should (be >= 1)
      _ = result.results.head shouldMatchTo
            SearchEntity.Project(
              matchingScore = MatchingScore(1f),
              slug = project2.slug,
              name = project2.name,
              visibility = project2.visibility,
              date = project2.dateCreated,
              dateModified = project2.dateModified,
              maybeCreator = project2.maybeCreator.map(_.name),
              keywords = project2.keywords.toList.sorted,
              maybeDescription = project2.maybeDescription,
              images = project2.images
            )
    } yield Succeeded
  }

  it should "return only public projects visible to the caller" in projectsDSConfig.use { implicit pcc =>
    val project1 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne
    val project2 = renkuProjectEntities(visibilityPrivate)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val person = personGen.generateOne

    for {
      _ <- uploadToProjects(person)
      _ <- provisionTestProjects(project1, project2)

      _ <- storeProjectViewed(person.maybeGitLabId.get, Instant.now().minus(1, ChronoUnit.DAYS), project1.slug)
      _ <- storeProjectViewed(person.maybeGitLabId.get, Instant.now(), project2.slug)

      criteria = Criteria(Set(EntityType.Project), AuthUser(person.maybeGitLabId.get, token), 10)
      result <- recentEntitiesFinder.findRecentlyViewedEntities(criteria)

      _ = result.results.size           shouldBe 1
      _ = result.pagingInfo.total.value shouldBe 1
      _ = result.results.head shouldMatchTo
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
    } yield Succeeded
  }

  it should "return only projects visible to the caller" in projectsDSConfig.use { implicit pcc =>
    val project1 = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne
    val project2 = renkuProjectEntities(visibilityPrivate)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .suchThat(_.maybeCreator.isDefined)
      .generateOne

    val person = project2.maybeCreator.get

    for {
      _ <- uploadToProjects(person)
      _ <- provisionTestProjects(project1, project2)

      _ <- storeProjectViewed(person.maybeGitLabId.get, Instant.now().minus(1, ChronoUnit.DAYS), project1.slug)
      _ <- storeProjectViewed(person.maybeGitLabId.get, Instant.now(), project2.slug)

      criteria = Criteria(Set(EntityType.Project), AuthUser(person.maybeGitLabId.get, token), 10)
      result <- recentEntitiesFinder.findRecentlyViewedEntities(criteria)

      _ = result.results.size           shouldBe 2
      _ = result.pagingInfo.total.value shouldBe 2
      _ = result.results.head shouldMatchTo
            SearchEntity.Project(
              matchingScore = MatchingScore(1f),
              slug = project2.slug,
              name = project2.name,
              visibility = project2.visibility,
              date = project2.dateCreated,
              dateModified = project2.dateModified,
              maybeCreator = project2.maybeCreator.map(_.name),
              keywords = project2.keywords.toList.sorted,
              maybeDescription = project2.maybeDescription,
              images = project2.images
            )
    } yield Succeeded
  }
}
