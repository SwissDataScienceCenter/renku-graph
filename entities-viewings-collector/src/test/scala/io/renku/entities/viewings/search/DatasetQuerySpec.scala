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

import io.renku.graph.model.entities
import io.renku.entities.search.model.{MatchingScore, Entity => SearchEntity}
import io.renku.entities.viewings.search.RecentEntitiesFinder.Criteria
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.datasets
import io.renku.http.server.security.model.AuthUser
import org.scalatest.OptionValues

import java.time.Instant
import java.time.temporal.ChronoUnit

class DatasetQuerySpec extends SearchTestBase with OptionValues {

  it should "return multiple datasets" in {
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

    upload(projectsDataset, person)
    provisionTestProjects(project1, project2).unsafeRunSync()

    storeProjectViewed(userId, Instant.now().minus(1, ChronoUnit.DAYS), project1.path)
    storeDatasetViewed(userId, Instant.now().minus(2, ChronoUnit.DAYS), dataset1.identification.identifier)
    storeProjectViewed(userId, Instant.now().minus(3, ChronoUnit.DAYS), project2.path)
    storeDatasetViewed(userId, Instant.now().minus(4, ChronoUnit.DAYS), dataset2.identification.identifier)

    val query = DatasetQuery.makeQuery(Criteria(Set.empty, AuthUser(userId, token), 5))

    val decoded = tsClient.queryExpecting[List[SearchEntity.Dataset]](query)(datasetDecoder).unsafeRunSync()

    decoded.size shouldBe 2
  }

  it should "find and decode datasets" in {
    val project = renkuProjectEntities(visibilityPublic)
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val dataset = project.to[entities.RenkuProject.WithoutParent].datasets.head

    val person = personGen.generateOne
    upload(projectsDataset, person)

    val userId = person.maybeGitLabId.get

    provisionTestProjects(project).unsafeRunSync()
    storeDatasetViewed(userId, Instant.now(), dataset.identification.identifier)

    val query = DatasetQuery.makeQuery(Criteria(Set.empty, AuthUser(userId, token), 5))

    val decoded = tsClient.queryExpecting[List[SearchEntity.Dataset]](query)(datasetDecoder).unsafeRunSync()
    decoded.head shouldMatchTo
      SearchEntity.Dataset(
        sameAs = Right(dataset.provenance.topmostSameAs),
        matchingScore = MatchingScore(1f),
        name = datasets.Name(dataset.identification.title.value),
        slug = datasets.Slug(dataset.identification.name.value),
        visibility = project.visibility,
        date = dataset.provenance.date,
        keywords = dataset.additionalInfo.keywords.sorted,
        maybeDescription = dataset.additionalInfo.maybeDescription,
        images = dataset.additionalInfo.images.map(_.uri),
        creators = dataset.provenance.creators.toList.map(_.name),
        exemplarProjectPath = project.path
      )
  }

  it should "return datasets for the given user only" in {
    val project1 = renkuProjectEntities(visibilityPublic)
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne
    val project2 = renkuProjectEntities(visibilityPublic)
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val person1 = personGen.generateOne
    val person2 = personGen.generateOne
    upload(projectsDataset, person1, person2)
    provisionTestProjects(project1, project2).unsafeRunSync()

    val dataset1 = project1.to[entities.RenkuProject.WithoutParent].datasets.head
    val dataset2 = project2.to[entities.RenkuProject.WithoutParent].datasets.head
    val userId1  = person1.maybeGitLabId.value
    val userId2  = person2.maybeGitLabId.value

    storeDatasetViewed(userId1, Instant.now(), dataset1.identification.identifier)
    storeDatasetViewed(userId2, Instant.now(), dataset2.identification.identifier)

    val query = DatasetQuery.makeQuery(Criteria(Set.empty, AuthUser(userId1, token), 5))

    val decoded = tsClient.queryExpecting[List[SearchEntity.Dataset]](query)(datasetDecoder).unsafeRunSync()
    decoded.head shouldMatchTo
      SearchEntity.Dataset(
        sameAs = Right(dataset1.provenance.topmostSameAs),
        matchingScore = MatchingScore(1f),
        name = datasets.Name(dataset1.identification.title.value),
        slug = datasets.Slug(dataset1.identification.name.value),
        visibility = project1.visibility,
        date = dataset1.provenance.date,
        keywords = dataset1.additionalInfo.keywords.sorted,
        maybeDescription = dataset1.additionalInfo.maybeDescription,
        images = dataset1.additionalInfo.images.map(_.uri),
        creators = dataset1.provenance.creators.toList.map(_.name),
        exemplarProjectPath = project1.path
      )
  }
}
