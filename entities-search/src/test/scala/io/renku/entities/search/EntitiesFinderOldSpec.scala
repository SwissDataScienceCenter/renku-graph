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

package io.renku.entities.search

import Criteria.Filters._
import Criteria.{Filters, Sort}
import EntityConverters._
import Generators._
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import io.renku.entities.search
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.generators.CommonGraphGenerators.sortingDirections
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model._
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.testentities.{Dataset, RenkuProject, StepPlan}
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.model._
import io.renku.http.rest.{SortBy, Sorting}
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.temporal.ChronoUnit.DAYS
import java.time.{Instant, LocalDate, ZoneOffset}
import scala.util.Random

class EntitiesFinderOldSpec
    extends AnyWordSpec
    with should.Matchers
    with EntitiesGenerators
    with FinderSpecOps
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets
    with IOSpec {

  protected[search] override def allEntitiesFrom(project: RenkuProject): List[model.Entity] =
    List(project.to[model.Entity.Project])
      .addAllDatasetsFromOld(project)
      .addAllPlansFrom(project)
      .addAllPersonsFrom(project)
      .distinct

  class OldTestCase extends TestCase {
    override val finder: EntitiesFinder[IO] =
      new EntitiesFinderImpl[IO](projectsDSConnectionInfo, EntitiesFinder.oldFinders)
  }

  "findEntities - no filters" should {

    "return all entities sorted by name if no query is given" in new OldTestCase {
      val project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      finder.findEntities(Criteria()).unsafeRunSync().results shouldBe allEntitiesFrom(project).sortBy(_.name)(
        nameOrdering
      )
    }

    "return all entities sorted by name if no query is given with modified plans" in new OldTestCase {
      val projectBase = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      val newPlan = projectBase.plans.head.createModification()
      val project = projectBase.copy(unlinkedPlans = newPlan.asInstanceOf[StepPlan] :: projectBase.unlinkedPlans)

      provisionTestProject(project).unsafeRunSync()

      val results = finder.findEntities(Criteria()).unsafeRunSync().results

      results shouldBe allEntitiesFrom(project).sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - with query filter" should {
    "return all entities sorted by two sorting parameters" in new OldTestCase {
      val projectDate = Instant.parse("2020-10-10T12:00:00Z")
      val otherDate   = Instant.parse("2021-06-15T00:00:00Z")
      val projectBase = renkuProjectEntities(
        visibilityGen = visibilityPublic,
        projectDateCreatedGen = fixed(projects.DateCreated(projectDate))
      )
        .modify(replaceProjectName(projects.Name("hello 1 hello 1 hello 1")))
        .withActivities(activityEntities(stepPlanEntities(fixed(plans.DateCreated(projectDate)))))
        .withDatasets(
          datasetEntities(provenanceNonModified)
            .modify(replaceDSName(datasets.Name("hello 2")))
            .modify(replaceDSDateCreatedOrPublished(otherDate))
        )
        .generateOne

      val newPlan = projectBase.plans.head
        .createModification()
        .asInstanceOf[StepPlan.Modified]
        .replacePlanDateCreated(plans.DateCreated(otherDate))
        .replacePlanName(plans.Name("hello 3"))
      val project = projectBase.copy(unlinkedPlans = newPlan :: projectBase.unlinkedPlans)

      provisionTestProject(project).unsafeRunSync()
      val results =
        finder
          .findEntities(
            Criteria(
              filters = Criteria.Filters(maybeQuery = Query("hello").some),
              sorting = Sorting(Sort.By(Sort.ByDate, SortBy.Direction.Asc), Sort.byNameAsc)
            )
          )
          .unsafeRunSync()
          .resultsWithSkippedMatchingScore

      implicit val entityOrdering: Ordering[model.Entity] =
        Ordering.by(e => s"${e.date}, ${e.name}".toLowerCase)

      val expected = allEntitiesFrom(project).filter(_.name.value.contains("hello")).sorted

      results shouldBe expected
    }

    "return entities which name matches the given query, sorted by name" in new OldTestCase {
      val query = nonBlankStrings(minLength = 6).generateOne

      val person = personEntities
        .map(replacePersonName(to = sentenceContaining(query).generateAs(persons.Name)))
        .generateOne

      val loneProject = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectName(sentenceContaining(query).generateAs(projects.Name)))
        .modify(creatorLens.modify(_ => person.some))
        .generateOne

      val dsAndProject @ _ -> dsProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            replaceDSName(to = sentenceContaining(query).generateAs(datasets.Name))
          )
        )
        .generateOne

      val planProject = renkuProjectEntities(visibilityPublic)
        .withActivities(
          activityEntities(
            stepPlanEntities().map(_.replacePlanName(to = sentenceContaining(query).generateAs(plans.Name)))
          )
        )
        .generateOne
      val plan :: Nil = planProject.plans

      val notMatchingProject = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      provisionTestProjects(loneProject, dsProject, planProject, notMatchingProject).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        loneProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset].copy(sameAs = Left(dsAndProject._1.identification.identifier)),
        (plan -> planProject).to[model.Entity.Workflow],
        person.to[model.Entity.Person]
      ).sortBy(_.name)(nameOrdering)
    }

    "return entities which keywords matches the given query, sorted by name" in new OldTestCase {
      val query = nonBlankStrings(minLength = 6).generateOne

      val soleProject = renkuProjectEntities(visibilityPublic)
        .modify(
          replaceProjectKeywords(to =
            Set(sentenceContaining(query).generateAs(projects.Keyword), projectKeywords.generateOne)
          )
        )
        .generateOne

      val dsAndProject @ _ -> dsProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            replaceDSKeywords(to =
              List(sentenceContaining(query).generateAs(datasets.Keyword), datasetKeywords.generateOne)
            )
          )
        )
        .generateOne

      val planProject = renkuProjectEntities(visibilityPublic)
        .withActivities(
          activityEntities(
            stepPlanEntities().map(
              _.replacePlanKeywords(to =
                List(sentenceContaining(query).generateAs(plans.Keyword), planKeywords.generateOne)
              )
            )
          )
        )
        .generateOne
      val plan :: Nil = planProject.plans

      provisionTestProjects(soleProject, dsProject, planProject, projectEntities(visibilityPublic).generateOne)
        .unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset].copy(sameAs = Left(dsAndProject._1.identification.identifier)),
        (plan -> planProject).to[model.Entity.Workflow]
      ).sortBy(_.name)(nameOrdering)
    }

    "return entities which description matches the given query, sorted by name" in new OldTestCase {
      val query = nonBlankStrings(minLength = 6).generateOne

      val soleProject = renkuProjectEntities(visibilityPublic)
        .modify(
          replaceProjectDesc(to = sentenceContaining(query).generateAs(projects.Description).some)
        )
        .generateOne

      val dsAndProject @ _ -> dsProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified)
            .modify(replaceDSDesc(to = sentenceContaining(query).generateAs(datasets.Description).some))
        )
        .generateOne

      val planProject = renkuProjectEntities(visibilityPublic)
        .withActivities(
          activityEntities(
            stepPlanEntities()
              .map(_.replacePlanDesc(to = sentenceContaining(query).generateAs(plans.Description).some))
          )
        )
        .generateOne
      val plan :: Nil = planProject.plans

      provisionTestProjects(soleProject, dsProject, planProject, projectEntities(visibilityPublic).generateOne)
        .unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset].copy(sameAs = Left(dsAndProject._1.identification.identifier)),
        (plan -> planProject).to[model.Entity.Workflow]
      ).sortBy(_.name)(nameOrdering)
    }

    "return project entities which namespace matches the given query, sorted by name" in new OldTestCase {
      val query = nonBlankStrings(minLength = 6).generateOne

      val soleProject = renkuProjectEntities(visibilityPublic)
        .modify(_.copy(path = projects.Path(s"$query/${relativePaths(maxSegments = 2).generateOne}")))
        .generateOne

      provisionTestProjects(soleProject, projectEntities(visibilityPublic).generateOne).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        soleProject.to[model.Entity.Project]
      ).sortBy(_.name)(nameOrdering)
    }

    "return entities which creator name matches the given query, sorted by name" in new OldTestCase {
      val query = nonBlankStrings(minLength = 6).generateOne

      val projectCreator = personEntities.generateOne.copy(name = sentenceContaining(query).generateAs(persons.Name))
      val soleProject = renkuProjectEntities(visibilityPublic)
        .modify(creatorLens.modify(_ => projectCreator.some))
        .generateOne

      val dsCreator = personEntities.generateOne.copy(name = sentenceContaining(query).generateAs(persons.Name))
      val dsAndProject @ _ -> dsProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            provenanceLens.modify(creatorsLens.modify(_ => NonEmptyList.of(dsCreator, personEntities.generateOne)))
          )
        )
        .generateOne

      provisionTestProjects(soleProject, dsProject, projectEntities(visibilityPublic).generateOne).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset].copy(sameAs = Left(dsAndProject._1.identification.identifier)),
        projectCreator.to[model.Entity.Person],
        dsCreator.to[model.Entity.Person]
      ).sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - with entity type filter" should {

    "return only projects when 'project' type given" in new OldTestCase {
      val project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(entityTypes = Set(EntityType.Project))))
        .unsafeRunSync()
        .results shouldBe List(project.to[model.Entity.Project]).sortBy(_.name)(nameOrdering)
    }

    "return only datasets when 'dataset' type given" in new OldTestCase {
      val dsAndProject @ _ -> project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(entityTypes = Set(EntityType.Dataset))))
        .unsafeRunSync()
        .results shouldBe List(
        dsAndProject.to[model.Entity.Dataset].copy(sameAs = Left(dsAndProject._1.identification.identifier))
      ).sortBy(_.name)(nameOrdering)
    }

    "return only workflows when 'workflow' type given" in new OldTestCase {
      val project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(entityTypes = Set(EntityType.Workflow))))
        .unsafeRunSync()
        .results shouldBe project.plans.map(_ -> project).map(_.to[model.Entity.Workflow]).sortBy(_.name)(nameOrdering)
    }

    "return entities of many types when multiple types given" in new OldTestCase {
      val person = personEntities.generateOne
      val project = renkuProjectEntities(visibilityPublic)
        .modify(creatorLens.modify(_ => person.some))
        .modify(removeMembers())
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(entityTypes = Set(EntityType.Person, EntityType.Project))))
        .unsafeRunSync()
        .results shouldBe List(
        project.to[model.Entity.Project],
        person.to[model.Entity.Person]
      ).sortBy(_.name)(nameOrdering)
    }

    "return multiple types datasets when 'person' type given" in new OldTestCase {
      val person = personEntities.generateOne
      val project = renkuProjectEntities(visibilityPublic)
        .modify(creatorLens.modify(_ => person.some))
        .modify(removeMembers())
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(entityTypes = Set(EntityType.Person))))
        .unsafeRunSync()
        .results shouldBe List(person.to[model.Entity.Person]).sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - with creator filter" should {

    "return entities with matching creator only" in new OldTestCase {
      val creator = personEntities.generateOne

      val soleProject = renkuProjectEntities(visibilityPublic)
        .modify(creatorLens.modify(_ => creator.some))
        .generateOne

      val dsAndProject @ _ -> dsProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            provenanceLens.modify(
              creatorsLens.modify(_ => NonEmptyList.of(personEntities.generateOne, creator))
            )
          )
        )
        .generateOne

      provisionTestProjects(soleProject, dsProject, projectEntities(visibilityPublic).generateOne).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(creators = Set(creator.name))))
        .unsafeRunSync()
        .results shouldBe List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset].copy(sameAs = Left(dsAndProject._1.identification.identifier)),
        creator.to[model.Entity.Person]
      ).sortBy(_.name)(nameOrdering)
    }

    "return entities creator matches in a case-insensitive way" in new OldTestCase {
      val creator = personEntities.generateOne

      val soleProject = renkuProjectEntities(visibilityPublic)
        .modify(creatorLens.modify(_ => creator.some))
        .generateOne

      val dsAndProject @ _ -> dsProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            provenanceLens.modify(
              creatorsLens.modify(_ => NonEmptyList.of(personEntities.generateOne, creator))
            )
          )
        )
        .generateOne

      provisionTestProjects(soleProject, dsProject).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(creators = Set(randomiseCases(creator.name.show).generateAs(persons.Name)))))
        .unsafeRunSync()
        .results shouldBe List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset].copy(sameAs = Left(dsAndProject._1.identification.identifier)),
        creator.to[model.Entity.Person]
      ).sortBy(_.name)(nameOrdering)
    }

    "return entities that matches at least one of the given creators" in new OldTestCase {

      val projectCreator = personEntities.generateOne
      val soleProject = renkuProjectEntities(visibilityPublic)
        .modify(creatorLens.modify(_ => projectCreator.some))
        .generateOne

      val dsCreator = personEntities.generateOne
      val dsAndProject @ _ -> dsProject = renkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            provenanceLens.modify(
              creatorsLens.modify(_ => NonEmptyList.of(personEntities.generateOne, dsCreator))
            )
          )
        )
        .generateOne

      provisionTestProjects(soleProject, dsProject, projectEntities(visibilityPublic).generateOne).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(creators = Set(projectCreator.name, dsCreator.name))))
        .unsafeRunSync()
        .results shouldBe List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset].copy(sameAs = Left(dsAndProject._1.identification.identifier)),
        projectCreator.to[model.Entity.Person],
        dsCreator.to[model.Entity.Person]
      ).sortBy(_.name)(nameOrdering)
    }

    "return no entities when there's no match on creator" in new OldTestCase {
      val _ -> project = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(creators = Set(personNames.generateOne))))
        .unsafeRunSync()
        .results shouldBe Nil
    }
  }

  "findEntities - with visibility filter" should {

    "return entities with matching visibility only" in new OldTestCase {

      val publicProject = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      val internalProject = renkuProjectEntities(fixed(projects.Visibility.Internal))
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val privateProject = renkuProjectEntities(fixed(projects.Visibility.Private))
        .modify(replaceMembers(to = Set(member)))
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      provisionTestProjects(publicProject, internalProject, privateProject).unsafeRunSync()

      finder
        .findEntities(
          Criteria(
            Filters(visibilities = Set(projects.Visibility.Public, projects.Visibility.Private)),
            maybeUser = member.toAuthUser.some,
            paging = PagingRequest(Page.first, PerPage(50))
          )
        )
        .unsafeRunSync()
        .results shouldBe allEntitiesFrom(publicProject)
        .addAllEntitiesOldFrom(privateProject)
        .addAllPersonsFrom(internalProject)
        .sortBy(_.name)(nameOrdering)
    }

    "return no entities when no match on visibility" in new OldTestCase {
      val _ -> project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(visibilities = visibilityNonPublic.generateSome.toSet)))
        .unsafeRunSync()
        .results shouldBe Nil
    }
  }

  "findEntities - with namespace filter" should {

    "return entities with matching namespace only" in new OldTestCase {

      val matchingProject = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      val nonMatchingProject = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      provisionTestProjects(matchingProject, nonMatchingProject).unsafeRunSync()

      finder
        .findEntities(
          Criteria(
            Filters(namespaces = Set(matchingProject.path.toNamespace)),
            paging = PagingRequest(Page.first, PerPage(50))
          )
        )
        .unsafeRunSync()
        .results shouldBe allEntitiesFrom(matchingProject)
        .removeAllPersons()
        .sortBy(_.name)(nameOrdering)
    }

    "return no namespace aware entities when no match on namespace" in new OldTestCase {
      val _ -> project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(namespaces = projectNamespaces.generateFixedSizeSet(1))))
        .unsafeRunSync()
        .results shouldBe Nil
    }
  }

  "findEntities - with 'since' filter" should {

    "return entities with date >= 'since'" in new OldTestCase {
      val since          = Filters.Since(sinceParams.generateOne.value minusDays 1)
      val sinceAsInstant = Instant.from(since.value atStartOfDay ZoneOffset.UTC)

      val projectDateCreated = timestamps(min = sinceAsInstant, max = Instant.now()).generateAs[projects.DateCreated]
      val ds -> project = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectDateCreated(to = projectDateCreated))
        .modify(replaceProjectDateModified(projectModifiedDates(projectDateCreated.value).generateOne))
        .withActivities(
          activityEntities(
            stepPlanEntities().map(
              _.replacePlanDateCreated(to =
                timestampsNotInTheFuture(butYoungerThan = projectDateCreated.value).generateAs[plans.DateCreated]
              )
            )
          )
        )
        .addDataset(
          datasetEntities(provenanceInternal)
            .modify(
              provenanceLens[Dataset.Provenance.Internal].modify(
                _.copy(date =
                  timestampsNotInTheFuture(butYoungerThan = projectDateCreated.value).generateAs[datasets.DateCreated]
                )
              )
            )
        )
        .generateOne
      val plan :: _ = project.plans

      provisionTestProject(project).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(maybeSince = since.some)))
        .unsafeRunSync()
        .results shouldBe List(
        project.to[model.Entity.Project],
        (ds -> project).to[model.Entity.Dataset].copy(sameAs = Left(ds.identification.identifier)),
        (plan -> project).to[model.Entity.Workflow]
      ).sortBy(_.name)(nameOrdering)
    }

    "return no entities with date < 'since'" in new OldTestCase {
      val since          = sinceParams.generateOne
      val sinceAsInstant = Instant.from(since.value atStartOfDay ZoneOffset.UTC)

      val projectDateCreated = timestamps(max = sinceAsInstant minus (2, DAYS)).generateAs(projects.DateCreated)
      val _ -> project = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectDateCreated(to = projectDateCreated))
        .modify(replaceProjectDateModified(projectModifiedDates(projectDateCreated.value).generateOne))
        .withActivities(
          activityEntities(
            stepPlanEntities().map(
              _.replacePlanDateCreated(to =
                timestamps(min = projectDateCreated.value, max = sinceAsInstant minus (1, DAYS))
                  .generateAs[plans.DateCreated]
              )
            )
          )
        )
        .addDataset(
          datasetEntities(provenanceInternal).modify(
            provenanceLens[Dataset.Provenance.Internal].modify(
              _.copy(date =
                timestamps(min = projectDateCreated.value, max = sinceAsInstant minus (1, DAYS))
                  .generateAs[datasets.DateCreated]
              )
            )
          )
        )
        .addDataset(
          datasetEntities(provenanceImportedExternal)
            .modify(
              provenanceLens[Dataset.Provenance.ImportedExternal]
                .modify(
                  _.copy(date =
                    timestamps(max = sinceAsInstant minus (1, DAYS))
                      .map(LocalDate.ofInstant(_, ZoneOffset.UTC))
                      .generateAs[datasets.DatePublished]
                  )
                )
            )
        )
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(maybeSince = since.some)))
        .unsafeRunSync()
        .results shouldBe Nil
    }

    "return entities with date >= 'since' - case of DatePublished" in new OldTestCase {
      val since          = sinceParams.generateOne
      val sinceAsInstant = Instant.from(since.value atStartOfDay ZoneOffset.UTC)

      val projectDateCreated = timestamps(max = sinceAsInstant minus (2, DAYS)).generateAs(projects.DateCreated)
      val matchingDS -> dsProject = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectDateCreated(projectDateCreated))
        .modify(replaceProjectDateModified(projectModifiedDates(projectDateCreated.value).generateOne))
        .addDataset(
          datasetEntities(provenanceImportedExternal)
            .modify(
              provenanceLens[Dataset.Provenance.ImportedExternal]
                .modify(
                  _.copy(date =
                    timestampsNotInTheFuture(butYoungerThan = sinceAsInstant)
                      .map(LocalDate.ofInstant(_, ZoneOffset.UTC))
                      .generateAs[datasets.DatePublished]
                  )
                )
            )
        )
        .generateOne

      provisionTestProject(dsProject).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(maybeSince = since.some)))
        .unsafeRunSync()
        .results shouldBe List(
        (matchingDS -> dsProject).to[model.Entity.Dataset].copy(sameAs = Left(matchingDS.identification.identifier))
      ).sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - with 'until' filter" should {

    "return entities with date <= 'until'" in new OldTestCase {
      val until          = untilParams.generateOne
      val untilAsInstant = Instant.from(until.value atStartOfDay ZoneOffset.UTC)

      val projectDateCreated = timestamps(max = untilAsInstant).generateAs[projects.DateCreated]
      val ds -> project = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectDateCreated(to = projectDateCreated))
        .modify(replaceProjectDateModified(projectModifiedDates(projectDateCreated.value).generateOne))
        .withActivities(
          activityEntities(
            stepPlanEntities().map(
              _.replacePlanDateCreated(to =
                timestamps(min = projectDateCreated.value, max = untilAsInstant).generateAs[plans.DateCreated]
              )
            )
          )
        )
        .addDataset(
          datasetEntities(provenanceInternal)
            .modify(
              provenanceLens[Dataset.Provenance.Internal].modify(
                _.copy(date =
                  timestamps(min = projectDateCreated.value, max = untilAsInstant).generateAs[datasets.DateCreated]
                )
              )
            )
        )
        .generateOne
      val plan :: _ = project.plans

      provisionTestProject(project).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(maybeUntil = until.some)))
        .unsafeRunSync()
        .results shouldBe List(
        project.to[model.Entity.Project],
        (ds -> project).to[model.Entity.Dataset].copy(sameAs = Left(ds.identification.identifier)),
        (plan -> project).to[model.Entity.Workflow]
      ).sortBy(_.name)(nameOrdering)
    }

    "return no entities with date > 'until'" in new OldTestCase {
      val until          = Until(untilParams.generateOne.value minusDays 2)
      val untilAsInstant = Instant.from(until.value atStartOfDay ZoneOffset.UTC)

      val projectDateCreated = timestampsNotInTheFuture(butYoungerThan = untilAsInstant plus (1, DAYS))
        .generateAs(projects.DateCreated)
      val _ -> project = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectDateCreated(to = projectDateCreated))
        .modify(replaceProjectDateModified(projectModifiedDates(projectDateCreated.value).generateOne))
        .withActivities(
          activityEntities(
            stepPlanEntities().map(
              _.replacePlanDateCreated(to =
                timestampsNotInTheFuture(butYoungerThan = projectDateCreated.value).generateAs[plans.DateCreated]
              )
            )
          )
        )
        .addDataset(
          datasetEntities(provenanceInternal).modify(
            provenanceLens[Dataset.Provenance.Internal].modify(
              _.copy(date =
                timestampsNotInTheFuture(butYoungerThan = projectDateCreated.value).generateAs[datasets.DateCreated]
              )
            )
          )
        )
        .addDataset(
          datasetEntities(provenanceImportedExternal)
            .modify(
              provenanceLens[Dataset.Provenance.ImportedExternal]
                .modify(
                  _.copy(date =
                    timestampsNotInTheFuture(butYoungerThan = projectDateCreated.value)
                      .map(LocalDate.ofInstant(_, ZoneOffset.UTC))
                      .generateAs[datasets.DatePublished]
                  )
                )
            )
        )
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(maybeUntil = until.some)))
        .unsafeRunSync()
        .results shouldBe Nil
    }

    "return entities with date <= 'until' - case of DatePublished" in new OldTestCase {
      val until          = Until(untilParams.generateOne.value minusDays 2)
      val untilAsInstant = Instant.from(until.value atStartOfDay ZoneOffset.UTC)

      val projectDateCreated =
        timestampsNotInTheFuture(butYoungerThan = untilAsInstant plus (1, DAYS)).generateAs(projects.DateCreated)
      val matchingDS -> dsProject = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectDateCreated(projectDateCreated))
        .modify(replaceProjectDateModified(projectModifiedDates(projectDateCreated.value).generateOne))
        .addDataset(
          datasetEntities(provenanceImportedExternal)
            .modify(
              provenanceLens[Dataset.Provenance.ImportedExternal]
                .modify(
                  _.copy(date =
                    timestamps(max = untilAsInstant)
                      .map(LocalDate.ofInstant(_, ZoneOffset.UTC))
                      .generateAs[datasets.DatePublished]
                  )
                )
            )
        )
        .generateOne

      provisionTestProject(dsProject).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(maybeUntil = until.some)))
        .unsafeRunSync()
        .results shouldBe List(
        (matchingDS -> dsProject).to[model.Entity.Dataset].copy(sameAs = Left(matchingDS.identification.identifier))
      ).sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - with both 'since' and 'until' filter" should {

    "return entities with date >= 'since' && date <= 'until'" in new OldTestCase {
      val sinceValue :: untilValue :: Nil = localDatesNotInTheFuture
        .generateFixedSizeList(ofSize = 2)
        .map(_ minusDays 1) // to prevent other parts of the test not to go into the future
        .sorted
      val since          = Since(sinceValue)
      val until          = Until(untilValue)
      val sinceAsInstant = Instant.from(sinceValue atStartOfDay ZoneOffset.UTC)
      val untilAsInstant = Instant.from(untilValue atStartOfDay ZoneOffset.UTC)

      val projectDateCreated = timestamps(min = sinceAsInstant, max = untilAsInstant).generateAs[projects.DateCreated]
      val dsInternal -> dsExternal -> _ -> _ -> project = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectDateCreated(to = projectDateCreated))
        .modify(replaceProjectDateModified(projectModifiedDates(projectDateCreated.value).generateOne))
        .withActivities(
          activityEntities(
            stepPlanEntities().map(
              _.replacePlanDateCreated(to =
                timestamps(min = projectDateCreated.value, max = untilAsInstant).generateAs[plans.DateCreated]
              )
            )
          )
        )
        .addDataset(
          datasetEntities(provenanceInternal)
            .modify(
              provenanceLens[Dataset.Provenance.Internal].modify(
                _.copy(date =
                  timestamps(min = projectDateCreated.value, max = untilAsInstant).generateAs[datasets.DateCreated]
                )
              )
            )
        )
        .addDataset(
          datasetEntities(provenanceImportedExternal)
            .modify(
              provenanceLens[Dataset.Provenance.ImportedExternal].modify(
                _.copy(date = localDates(min = sinceValue, max = untilValue).generateAs[datasets.DatePublished])
              )
            )
        )
        .addDataset(
          datasetEntities(provenanceImportedExternal)
            .modify(
              provenanceLens[Dataset.Provenance.ImportedExternal].modify(
                _.copy(date = localDates(max = sinceValue minusDays 1).generateAs[datasets.DatePublished])
              )
            )
        )
        .addDataset(
          datasetEntities(provenanceImportedExternal)
            .modify(
              provenanceLens[Dataset.Provenance.ImportedExternal].modify(
                _.copy(date =
                  localDates(min = untilValue plusDays 1, max = LocalDate.now()).generateAs[datasets.DatePublished]
                )
              )
            )
        )
        .generateOne
      val plan :: _ = project.plans

      provisionTestProject(project).unsafeRunSync()

      finder
        .findEntities(Criteria(Filters(maybeSince = since.some, maybeUntil = until.some)))
        .unsafeRunSync()
        .results shouldBe List(
        project.to[model.Entity.Project],
        (dsInternal -> project).to[model.Entity.Dataset].copy(sameAs = Left(dsInternal.identification.identifier)),
        (dsExternal -> project).to[model.Entity.Dataset].copy(sameAs = Left(dsExternal.identification.identifier)),
        (plan -> project).to[model.Entity.Workflow]
      ).sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - with sorting" should {

    "be sorting by Name if requested in case-insensitive way" in new OldTestCase {
      val commonPart = nonEmptyStrings().generateOne
      val project = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectName(projects.Name(s"a$commonPart")))
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified).modify(replaceDSName(datasets.Name(s"B$commonPart"))))
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      val direction = sortingDirections.generateOne

      finder
        .findEntities(Criteria(sorting = Sorting(Sort.By(Sort.ByName, direction))))
        .unsafeRunSync()
        .results shouldBe allEntitiesFrom(project).sortBy(_.name)(nameOrdering).use(direction)
    }

    "be sorting by Date if requested" in new OldTestCase {
      val project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()), activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceImportedExternal))
        .withDatasets(datasetEntities(provenanceInternal))
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      val direction = sortingDirections.generateOne

      val results = finder
        .findEntities(Criteria(sorting = Sorting(Sort.By(Sort.ByDate, direction))))
        .unsafeRunSync()
        .results

      val expectedPersons = List.empty[model.Entity].addAllPersonsFrom(project).toSet

      val expectedSorted = List(project.to[model.Entity])
        .addAllDatasetsFromOld(project)
        .addAllPlansFrom(project)
        .sortBy(_.dateAsInstant)
        .use(direction)

      val (sorted, persons) = if (direction == SortBy.Direction.Asc) {
        val (persons, sorted) = results splitAt expectedPersons.size
        sorted -> persons
      } else results splitAt expectedSorted.size

      sorted        shouldBe expectedSorted
      persons.toSet shouldBe expectedPersons
    }

    "be sorting by Matching Score if requested" in new OldTestCase {

      val query = nonBlankStrings(minLength = 6).generateOne

      val ds -> project = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectName(to = projects.Name(query.value)))
        .withActivities(activityEntities(stepPlanEntities().map(_.replacePlanName(to = plans.Name(s"smth $query")))))
        .addDataset(
          datasetEntities(provenanceNonModified)
            .modify(replaceDSName(to = sentenceContaining(query).generateAs(datasets.Name)))
        )
        .generateOne
      val plan :: Nil = project.plans

      provisionTestProject(project).unsafeRunSync()

      val direction = sortingDirections.generateOne

      finder
        .findEntities(
          Criteria(Filters(maybeQuery = Filters.Query(query.value).some),
                   Sorting(Sort.By(Sort.ByMatchingScore, direction))
          )
        )
        .unsafeRunSync()
        .resultsWithSkippedMatchingScore shouldBe List(
        project.to[model.Entity.Project], // should have the highest score as its name is the query
        (plan -> project).to[model.Entity.Workflow], // should have higher score as its name is the query with a prefix
        (ds -> project).to[model.Entity.Dataset].copy(sameAs = Left(ds.identification.identifier))
      ).use(direction).reverse
    }
  }

  "findEntities - with paging" should {

    val project = renkuProjectEntities(visibilityPublic)
      .withDatasets(
        datasetEntities(provenanceNonModified),
        datasetEntities(provenanceNonModified)
      )
      .generateOne

    "return the only page" in new OldTestCase {

      provisionTestProject(project).unsafeRunSync()

      val paging = PagingRequest(Page(1), PerPage(3))

      val results = finder
        .findEntities(
          Criteria(paging = paging, filters = Filters(entityTypes = Set(EntityType.Project, EntityType.Dataset)))
        )
        .unsafeRunSync()

      results.pagingInfo.pagingRequest shouldBe paging
      results.pagingInfo.total         shouldBe Total(3)
      results.results shouldBe List(project.to[model.Entity.Project])
        .addAllDatasetsFromOld(project)
        .sortBy(_.name)(nameOrdering)
    }

    "return the requested page with info if there are more" in new OldTestCase {

      provisionTestProject(project).unsafeRunSync()

      val paging = PagingRequest(Page(Random.nextInt(3) + 1), PerPage(1))

      val results = finder
        .findEntities(
          Criteria(paging = paging, filters = Filters(entityTypes = Set(EntityType.Project, EntityType.Dataset)))
        )
        .unsafeRunSync()

      results.pagingInfo.pagingRequest shouldBe paging
      results.pagingInfo.total         shouldBe Total(3)
      results.results shouldBe List(project.to[model.Entity.Project])
        .addAllDatasetsFromOld(project)
        .sortBy(_.name)(nameOrdering)
        .get(paging.page.value - 1)
        .toList
    }

    "return no results if non-existing page requested" in new OldTestCase {

      provisionTestProject(project).unsafeRunSync()

      val paging = PagingRequest(Page(4), PerPage(1))

      val results = finder
        .findEntities(
          Criteria(paging = paging, filters = Filters(entityTypes = Set(EntityType.Project, EntityType.Dataset)))
        )
        .unsafeRunSync()

      results.pagingInfo.pagingRequest shouldBe paging
      results.pagingInfo.total         shouldBe Total(3)
      results.results                  shouldBe Nil
    }
  }

  "findEntities - with images" should {

    "return images if present" in new OldTestCase {
      val project = renkuProjectEntities(visibilityPublic)
        .suchThat(_.images.nonEmpty)
        .generateOne

      provisionTestProject(project).unsafeRunSync()

      val results = finder.findEntities(Criteria()).unsafeRunSync()
      val images  = results.results.collect { case e: search.model.Entity.Project => e.images }.flatten
      images shouldBe project.images
    }
  }

  "findEntities - security" should {

    val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne

    val privateProject = renkuProjectEntities(fixed(Visibility.Private))
      .modify(replaceMembers(to = Set(member)))
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val internalProject = renkuProjectEntities(fixed(Visibility.Internal))
      .modify(replaceMembers(to = Set(member)))
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val publicProject = renkuProjectEntities(visibilityPublic)
      .modify(replaceMembers(to = Set(member)))
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    "return public entities only if no auth user is given" in new OldTestCase {

      provisionTestProjects(privateProject, internalProject, publicProject).unsafeRunSync()

      finder.findEntities(Criteria()).unsafeRunSync().results shouldBe List
        .empty[model.Entity]
        .addAllEntitiesOldFrom(publicProject)
        .addAllPersonsFrom(internalProject)
        .addAllPersonsFrom(privateProject)
        .sortBy(_.name)(nameOrdering)
    }

    "return public and internal entities only if auth user is given" in new OldTestCase {

      provisionTestProjects(privateProject, internalProject, publicProject).unsafeRunSync()

      finder
        .findEntities(
          Criteria(maybeUser = personEntities(personGitLabIds.toGeneratorOfSomes).generateSome.map(_.toAuthUser),
                   paging = PagingRequest.default.copy(perPage = PerPage(50))
          )
        )
        .unsafeRunSync()
        .results shouldBe List
        .empty[model.Entity]
        .addAllEntitiesOldFrom(publicProject)
        .addAllEntitiesOldFrom(internalProject)
        .addAllPersonsFrom(privateProject)
        .sortBy(_.name)(nameOrdering)
    }

    "return any visibility entities if the given auth user has access to them" in new OldTestCase {

      provisionTestProjects(privateProject, internalProject, publicProject).unsafeRunSync()

      finder
        .findEntities(
          Criteria(maybeUser = member.toAuthUser.some, paging = PagingRequest.default.copy(perPage = PerPage(50)))
        )
        .unsafeRunSync()
        .results shouldBe List
        .empty[model.Entity]
        .addAllEntitiesOldFrom(publicProject)
        .addAllEntitiesOldFrom(internalProject)
        .addAllEntitiesOldFrom(privateProject)
        .sortBy(_.name)(nameOrdering)
    }
  }
}
