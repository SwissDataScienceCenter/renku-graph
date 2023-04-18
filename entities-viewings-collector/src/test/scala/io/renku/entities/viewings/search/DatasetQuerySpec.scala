package io.renku.entities.viewings.search

import io.renku.entities.search.model.{MatchingScore, Entity => SearchEntity}
import io.renku.entities.viewings.search.RecentEntitiesFinder.Criteria
import io.renku.generators.Generators.Implicits._
import io.renku.http.server.security.model.AuthUser

import java.time.Instant

class DatasetQuerySpec extends SearchTestBase {

  it should "find and decode projects" in {
    val project = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val person = personGen.generateOne
    upload(projectsDataset, person)

    val userId = person.maybeGitLabId.get

    provisionTestProjects(project).unsafeRunSync()
    storeDatasetViewed(userId, Instant.now(), project.datasets.head.identification.identifier)

    val query = DatasetQuery.makeQuery(Criteria(Set.empty, AuthUser(userId, token), 5))

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
}
