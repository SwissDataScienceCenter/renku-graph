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
import io.renku.entities.viewings.search.RecentEntitiesFinder.Criteria
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.GraphJenaSpec
import org.scalatest.{OptionValues, Succeeded}

import java.time.Instant
import java.time.temporal.ChronoUnit

class DatasetQuerySpec extends SearchSpec with GraphJenaSpec with OptionValues {

  it should "return multiple datasets" in projectsDSConfig.use { implicit pcc =>
    val project1 = renkuProjectEntities(visibilityPublic)
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne
    val project2 = renkuProjectEntities(visibilityPublic)
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val dataset1 = project1.to[entities.RenkuProject.WithoutParent].datasets.head
    val dataset2 = project2.to[entities.RenkuProject.WithoutParent].datasets.head

    val person = personGen.generateOne
    val userId = person.maybeGitLabId.value

    for {
      _ <- uploadToProjects(person)
      _ <- provisionTestProjects(project1, project2)

      _ <- storeProjectViewed(userId, Instant.now().minus(1, ChronoUnit.DAYS), project1.slug)
      _ <- storeDatasetViewed(userId, Instant.now().minus(2, ChronoUnit.DAYS), dataset1.identification.identifier)
      _ <- storeProjectViewed(userId, Instant.now().minus(3, ChronoUnit.DAYS), project2.slug)
      _ <- storeDatasetViewed(userId, Instant.now().minus(4, ChronoUnit.DAYS), dataset2.identification.identifier)

      query = DatasetQuery.makeQuery(Criteria(Set.empty, AuthUser(userId, token), 5))

      _ <- tsClient.queryExpecting[List[SearchEntity.Dataset]](query)(datasetDecoder).asserting(_.size shouldBe 2)
    } yield Succeeded
  }

  it should "find and decode datasets" in projectsDSConfig.use { implicit pcc =>
    val project = renkuProjectEntities(visibilityPublic)
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val dataset = project.to[entities.RenkuProject.WithoutParent].datasets.head

    val person = personGen.generateOne
    for {
      _ <- uploadToProjects(person)

      userId = person.maybeGitLabId.get

      _ <- provisionTestProjects(project)
      _ <- storeDatasetViewed(userId, Instant.now(), dataset.identification.identifier)

      query = DatasetQuery.makeQuery(Criteria(Set.empty, AuthUser(userId, token), 5))

      _ <- tsClient.queryExpecting[List[SearchEntity.Dataset]](query)(datasetDecoder).asserting {
             _.head shouldMatchTo
               SearchEntity.Dataset(
                 matchingScore = MatchingScore(1f),
                 sameAs = dataset.provenance.topmostSameAs,
                 slug = dataset.identification.slug,
                 name = dataset.identification.name,
                 visibility = project.visibility,
                 date = dataset.provenance.date,
                 dateModified = None,
                 keywords = dataset.additionalInfo.keywords.sorted,
                 maybeDescription = dataset.additionalInfo.maybeDescription,
                 images = dataset.additionalInfo.images.map(_.uri),
                 creators = dataset.provenance.creators.toList.map(_.name),
                 exemplarProjectSlug = project.slug
               )
           }
    } yield Succeeded
  }

  it should "return datasets for the given user only" in projectsDSConfig.use { implicit pcc =>
    val project1 = renkuProjectEntities(visibilityPublic)
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne
    val project2 = renkuProjectEntities(visibilityPublic)
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val person1 = personGen.generateOne
    val person2 = personGen.generateOne
    for {
      _ <- uploadToProjects(person1, person2)
      _ <- provisionTestProjects(project1, project2)

      dataset1 = project1.to[entities.RenkuProject.WithoutParent].datasets.head
      dataset2 = project2.to[entities.RenkuProject.WithoutParent].datasets.head
      userId1  = person1.maybeGitLabId.value
      userId2  = person2.maybeGitLabId.value

      _ <- storeDatasetViewed(userId1, Instant.now(), dataset1.identification.identifier)
      _ <- storeDatasetViewed(userId2, Instant.now(), dataset2.identification.identifier)

      query = DatasetQuery.makeQuery(Criteria(Set.empty, AuthUser(userId1, token), 5))

      _ <- tsClient.queryExpecting[List[SearchEntity.Dataset]](query)(datasetDecoder).asserting {
             _.head shouldMatchTo
               SearchEntity.Dataset(
                 matchingScore = MatchingScore(1f),
                 sameAs = dataset1.provenance.topmostSameAs,
                 slug = dataset1.identification.slug,
                 name = dataset1.identification.name,
                 visibility = project1.visibility,
                 date = dataset1.provenance.date,
                 dateModified = None,
                 keywords = dataset1.additionalInfo.keywords.sorted,
                 maybeDescription = dataset1.additionalInfo.maybeDescription,
                 images = dataset1.additionalInfo.images.map(_.uri),
                 creators = dataset1.provenance.creators.toList.map(_.name),
                 exemplarProjectSlug = project1.slug
               )
           }
    } yield Succeeded
  }
}
