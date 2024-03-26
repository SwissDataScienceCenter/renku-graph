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

package io.renku.entities.search

import cats.data.NonEmptyList
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.search
import io.renku.entities.search.Criteria.Filters._
import io.renku.entities.search.Criteria.{Filters, Sort}
import io.renku.entities.search.EntityConverters._
import io.renku.entities.search.Generators._
import io.renku.entities.search.diff.SearchDiffInstances
import io.renku.entities.search.model.Entity
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.http.client.HttpClientGenerators.sortingDirections
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model._
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.testentities.{Dataset, StepPlan}
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.model._
import io.renku.http.rest.{SortBy, Sorting}
import io.renku.triplesstore.GraphJenaSpec
import org.scalacheck.Gen.alphaLowerChar
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import java.time.temporal.ChronoUnit.DAYS
import java.time.{Instant, LocalDate, ZoneOffset}
import scala.util.Random

class EntitiesFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with FinderSpec
    with TestSearchInfoDatasets
    with EntitiesGenerators
    with SearchDiffInstances
    with AdditionalMatchers
    with should.Matchers {

  "findEntities - no filters" should {

    "return all entities sorted by name if no query is given" in projectsDSConfig.use { implicit pcc =>
      val project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      for {
        _     <- provisionTestProject(project)
        found <- entitiesFinder.findEntities(Criteria())

        expected = allEntitiesFrom(project).sortBy(_.name)(nameOrdering)
        _        = found.results shouldMatchTo expected
      } yield ()
    }

    "return all entities sorted by name if no query is given with modified plans" in projectsDSConfig.use {
      implicit pcc =>
        val projectBase = renkuProjectEntities(visibilityPublic)
          .withActivities(activityEntities(stepPlanEntities()))
          .withDatasets(datasetEntities(provenanceNonModified))
          .generateOne

        val newPlan = projectBase.plans.head.createModification()
        val project = projectBase.copy(unlinkedPlans = newPlan.asInstanceOf[StepPlan] :: projectBase.unlinkedPlans)

        for {
          _       <- provisionTestProject(project)
          results <- entitiesFinder.findEntities(Criteria())
        } yield results.results shouldMatchTo allEntitiesFrom(project).sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - with query filter" should {
    "return all entities sorted by two sorting parameters" in projectsDSConfig.use { implicit pcc =>
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

      // expected date ordering is to use dateModified when available
      implicit val entityOrdering: Ordering[model.Entity] =
        Ordering.by {
          case d: Entity.Dataset =>
            val date = d.dateModified.getOrElse(d.date).toString
            s"$date, ${d.name}".toLowerCase
          case p: Entity.Project =>
            val date = p.dateModified.toString
            s"$date, ${p.name}".toLowerCase
          case e =>
            val date = e.date.toString
            s"$date, ${e.name}".toLowerCase
        }

      for {
        _ <- provisionTestProject(project)
        results <- entitiesFinder
                     .findEntities(
                       Criteria(
                         filters = Criteria.Filters(maybeQuery = Query("hello").some),
                         sorting = Sorting(Sort.By(Sort.ByDate, SortBy.Direction.Asc), Sort.byNameAsc)
                       )
                     )
                     .map(_.resultsWithSkippedMatchingScore)
      } yield results shouldMatchTo allEntitiesFrom(project).filter(nameOrSlugContains("hello")).sorted
    }

    "return entities which name matches the given query, sorted by name" in projectsDSConfig.use { implicit pcc =>
      val query = nonBlankStrings(minLength = 6, charsGenerator = alphaLowerChar).generateOne

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

      for {
        _ <- List(loneProject, dsProject, planProject, notMatchingProject).traverse_(provisionTestProject(_))
        results <- entitiesFinder
                     .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
                     .map(_.resultsWithSkippedMatchingScore)
      } yield results shouldMatchTo List(
        loneProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset],
        (plan -> planProject).to[model.Entity.Workflow],
        person.to[model.Entity.Person]
      ).sortBy(_.name)(nameOrdering)
    }

    "return entities which keywords matches the given query, sorted by name" in projectsDSConfig.use { implicit pcc =>
      val query = nonBlankStrings(minLength = 6, charsGenerator = alphaLowerChar).generateOne

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

      for {
        _ <- List(soleProject, dsProject, planProject, projectEntities(visibilityPublic).generateOne)
               .traverse_(provisionTestProject(_))
        results <- entitiesFinder
                     .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
                     .map(_.resultsWithSkippedMatchingScore)
      } yield results shouldMatchTo List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset],
        (plan -> planProject).to[model.Entity.Workflow]
      ).sortBy(_.name)(nameOrdering)
    }

    "return entities which description matches the given query, sorted by name" in projectsDSConfig.use {
      implicit pcc =>
        val query = nonBlankStrings(minLength = 6, charsGenerator = alphaLowerChar).generateOne

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

        for {
          _ <- List(soleProject, dsProject, planProject, projectEntities(visibilityPublic).generateOne)
                 .traverse_(provisionTestProject)
          results <- entitiesFinder
                       .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
                       .map(_.resultsWithSkippedMatchingScore)
        } yield results shouldBe List(
          soleProject.to[model.Entity.Project],
          dsAndProject.to[model.Entity.Dataset],
          (plan -> planProject).to[model.Entity.Workflow]
        ).sortBy(_.name)(nameOrdering)
    }

    "return project entities which namespace matches the given query, sorted by name" in projectsDSConfig.use {
      implicit pcc =>
        val query = nonBlankStrings(minLength = 6, charsGenerator = alphaLowerChar).generateOne

        val soleProject = renkuProjectEntities(visibilityPublic)
          .modify(_.copy(slug = projects.Slug(s"$query/${relativePaths(maxSegments = 2).generateOne}")))
          .generateOne

        for {
          _ <- List(soleProject, projectEntities(visibilityPublic).generateOne).traverse_(provisionTestProject)
          results <- entitiesFinder
                       .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
                       .map(_.resultsWithSkippedMatchingScore)
        } yield results shouldMatchTo List(soleProject.to[model.Entity.Project]).sortBy(_.name)(nameOrdering)

    }

    "return entities which creator name matches the given query, sorted by name" in projectsDSConfig.use {
      implicit pcc =>
        val query = nonBlankStrings(minLength = 6, charsGenerator = alphaLowerChar).generateOne

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

        for {
          _ <- List(soleProject, dsProject, projectEntities(visibilityPublic).generateOne)
                 .traverse_(provisionTestProject(_))
          results <- entitiesFinder
                       .findEntities(Criteria(Filters(maybeQuery = Query(query.value).some)))
                       .map(_.resultsWithSkippedMatchingScore)
        } yield results shouldMatchTo List(
          soleProject.to[model.Entity.Project],
          dsAndProject.to[model.Entity.Dataset],
          projectCreator.to[model.Entity.Person],
          dsCreator.to[model.Entity.Person]
        ).sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - with entity type filter" should {

    "return only projects when 'project' type given" in projectsDSConfig.use { implicit pcc =>
      val project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      for {
        _ <- provisionTestProject(project)
        results <- entitiesFinder
                     .findEntities(Criteria(Filters(entityTypes = Set(EntityType.Project))))
                     .map(_.results)
      } yield results shouldMatchTo List(project.to[model.Entity.Project]).sortBy(_.name)(nameOrdering)
    }

    "return only datasets when 'dataset' type given" in projectsDSConfig.use { implicit pcc =>
      val dsAndProject @ _ -> project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      for {
        _       <- List(project).traverse_(provisionTestProject)
        results <- entitiesFinder.findEntities(Criteria(Filters(entityTypes = Set(EntityType.Dataset))))
      } yield results.results shouldMatchTo List(dsAndProject.to[model.Entity.Dataset]).sortBy(_.name)(nameOrdering)
    }

    "return only workflows when 'workflow' type given" in projectsDSConfig.use { implicit pcc =>
      val project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      for {
        _ <- List(project).traverse_(provisionTestProject)
        results <- entitiesFinder
                     .findEntities(Criteria(Filters(entityTypes = Set(EntityType.Workflow))))
      } yield results.results shouldBe project.plans
        .map(_ -> project)
        .map(_.to[model.Entity.Workflow])
        .sortBy(_.name)(nameOrdering)
    }

    "return entities of many types when multiple types given" in projectsDSConfig.use { implicit pcc =>
      val person = personEntities.generateOne
      val project = renkuProjectEntities(visibilityPublic)
        .modify(creatorLens.modify(_ => person.some))
        .modify(removeMembers())
        .generateOne

      for {
        _ <- List(project).traverse_(provisionTestProject)
        results <- entitiesFinder
                     .findEntities(Criteria(Filters(entityTypes = Set(EntityType.Person, EntityType.Project))))
      } yield results.results shouldBe List(
        project.to[model.Entity.Project],
        person.to[model.Entity.Person]
      ).sortBy(_.name)(nameOrdering)
    }

    "return multiple types datasets when 'person' type given" in projectsDSConfig.use { implicit pcc =>
      val person = personEntities.generateOne
      val project = renkuProjectEntities(visibilityPublic)
        .modify(creatorLens.modify(_ => person.some))
        .modify(removeMembers())
        .generateOne

      for {
        _ <- List(project).traverse_(provisionTestProject)
        results <- entitiesFinder
                     .findEntities(Criteria(Filters(entityTypes = Set(EntityType.Person))))
      } yield results.results shouldBe List(person.to[model.Entity.Person]).sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - with creator filter" should {

    "return entities with matching creator only" in projectsDSConfig.use { implicit pcc =>
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

      for {
        _ <- List(soleProject, dsProject, projectEntities(visibilityPublic).generateOne).traverse_(provisionTestProject)
        results <- entitiesFinder
                     .findEntities(Criteria(Filters(creators = Set(creator.name))))
      } yield results.results shouldMatchTo List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset],
        creator.to[model.Entity.Person]
      ).sortBy(_.name)(nameOrdering)
    }

    "return entities creator matches in a case-insensitive way" in projectsDSConfig.use { implicit pcc =>
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

      for {
        _ <- List(soleProject, dsProject).traverse_(provisionTestProject)
        results <-
          entitiesFinder
            .findEntities(Criteria(Filters(creators = Set(randomiseCases(creator.name.show).generateAs(persons.Name)))))
      } yield results.results shouldBe List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset],
        creator.to[model.Entity.Person]
      ).sortBy(_.name)(nameOrdering)
    }

    "return entities that matches at least one of the given creators" in projectsDSConfig.use { implicit pcc =>
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

      for {
        _ <- List(soleProject, dsProject, projectEntities(visibilityPublic).generateOne).traverse_(provisionTestProject)
        results <- entitiesFinder
                     .findEntities(Criteria(Filters(creators = Set(projectCreator.name, dsCreator.name))))
      } yield results.results shouldBe List(
        soleProject.to[model.Entity.Project],
        dsAndProject.to[model.Entity.Dataset],
        projectCreator.to[model.Entity.Person],
        dsCreator.to[model.Entity.Person]
      ).sortBy(_.name)(nameOrdering)
    }

    "return no entities when there's no match on creator" in projectsDSConfig.use { implicit pcc =>
      val _ -> project = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      for {
        _ <- List(project).traverse_(provisionTestProject)
        _ <- entitiesFinder
               .findEntities(Criteria(Filters(creators = Set(personNames.generateOne))))
               .asserting(_.results shouldBe Nil)
      } yield ()
    }
  }

  "findEntities - with role filter" should {

    "return project entities where the given user has the given role" in projectsDSConfig.use { implicit pcc =>
      val memberId = personGitLabIds.generateOne
      val member   = projectMemberEntities(memberId.some).generateOne

      val project = renkuProjectEntities(anyVisibility)
        .modify(replaceMembers(Set(member)))
        .generateOne

      for {
        _ <- provisionTestProjects(project, projectEntities(visibilityPublic).generateOne)
        results <-
          entitiesFinder.findEntities(
            Criteria(Filters(roles = Set(member.role)), maybeUser = authUsers.generateOne.copy(id = memberId).some)
          )
      } yield results.results shouldMatchTo List(
        project.to[model.Entity.Project].some,
        Option.when(member.role == projects.Role.Owner)(member.person.to[model.Entity.Person])
      ).flatten.sortBy(_.name)(nameOrdering)
    }

    "return project entities where the given user has the given role - case when filtering on visibility is set" in projectsDSConfig
      .use { implicit pcc =>
        val memberId = personGitLabIds.generateOne
        val member   = projectMemberEntities(memberId.some).generateOne

        val publicProject = renkuProjectEntities(visibilityPublic)
          .modify(replaceMembers(Set(member)))
          .generateOne
        val nonPublicProject = renkuProjectEntities(visibilityNonPublic)
          .modify(replaceMembers(Set(member)))
          .generateOne

        for {
          _ <- provisionTestProjects(publicProject, nonPublicProject)
          results <- entitiesFinder.findEntities(
                       Criteria(Filters(roles = Set(member.role), visibilities = Set(projects.Visibility.Public)),
                                maybeUser = authUsers.generateOne.copy(id = memberId).some
                       )
                     )
        } yield results.results shouldMatchTo List(
          publicProject.to[model.Entity.Project].some,
          Option.when(member.role == projects.Role.Owner)(member.person.to[model.Entity.Person])
        ).flatten.sortBy(_.name)(nameOrdering)
      }

    "return dataset entities where the given user has the given role" in projectsDSConfig.use { implicit pcc =>
      val memberId = personGitLabIds.generateOne
      val member   = projectMemberEntities(memberId.some).generateOne

      val dsAndProject @ _ -> project = renkuProjectEntities(anyVisibility)
        .modify(replaceMembers(Set(member)))
        .addDataset(
          datasetEntities(provenanceNonModified)
            .modify(replaceDSCreators(NonEmptyList.of(personEntities.generateOne, member.person)))
        )
        .generateOne

      for {
        _ <- provisionTestProjects(project, projectEntities(visibilityPublic).generateOne)
        results <-
          entitiesFinder.findEntities(
            Criteria(Filters(roles = Set(member.role)), maybeUser = authUsers.generateOne.copy(id = memberId).some)
          )
      } yield results.results shouldMatchTo List(
        project.to[model.Entity.Project].some,
        dsAndProject.to[model.Entity.Dataset].some,
        Option.when(member.role == projects.Role.Owner)(member.person.to[model.Entity.Person])
      ).flatten.sortBy(_.name)(nameOrdering)
    }

    "return dataset entities where the given user has the given role - case when filtering on visibility is set" in projectsDSConfig
      .use { implicit pcc =>
        val memberId = personGitLabIds.generateOne
        val member   = projectMemberEntities(memberId.some).generateOne

        val dsAndPublicProject @ _ -> publicProject = renkuProjectEntities(visibilityPublic)
          .modify(replaceMembers(Set(member)))
          .addDataset(datasetEntities(provenanceNonModified).modify(replaceDSCreators(NonEmptyList.of(member.person))))
          .generateOne
        val _ -> nonPublicProject = renkuProjectEntities(visibilityNonPublic)
          .modify(replaceMembers(Set(member)))
          .addDataset(datasetEntities(provenanceNonModified).modify(replaceDSCreators(NonEmptyList.of(member.person))))
          .generateOne

        for {
          _ <- provisionTestProjects(publicProject, nonPublicProject)
          results <- entitiesFinder.findEntities(
                       Criteria(Filters(roles = Set(member.role), visibilities = Set(projects.Visibility.Public)),
                                maybeUser = authUsers.generateOne.copy(id = memberId).some
                       )
                     )
        } yield results.results shouldMatchTo List(
          publicProject.to[model.Entity.Project].some,
          dsAndPublicProject.to[model.Entity.Dataset].some,
          Option.when(member.role == projects.Role.Owner)(member.person.to[model.Entity.Person])
        ).flatten.sortBy(_.name)(nameOrdering)
      }

    "return workflow entities where the given user has the given role" in projectsDSConfig.use { implicit pcc =>
      val memberId = personGitLabIds.generateOne
      val member   = projectMemberEntities(memberId.some).generateOne

      val project = renkuProjectEntities(anyVisibility)
        .modify(replaceMembers(Set(member)))
        .withActivities(
          activityEntities(stepPlanEntities().map(_.replaceCreators(List(personEntities.generateOne, member.person))))
        )
        .generateOne

      for {
        _ <- provisionTestProjects(project, projectEntities(visibilityPublic).generateOne)
        results <-
          entitiesFinder.findEntities(
            Criteria(Filters(roles = Set(member.role)), maybeUser = authUsers.generateOne.copy(id = memberId).some)
          )
      } yield results.results shouldMatchTo List(
        project.to[model.Entity.Project].some,
        (project.plans.head -> project).to[model.Entity.Workflow].some,
        Option.when(member.role == projects.Role.Owner)(member.person.to[model.Entity.Person])
      ).flatten.sortBy(_.name)(nameOrdering)
    }

    "return workflow entities where the given user has the given role - case when filtering on visibility is set" in projectsDSConfig
      .use { implicit pcc =>
        val memberId = personGitLabIds.generateOne
        val member   = projectMemberEntities(memberId.some).generateOne

        val publicProject = renkuProjectEntities(visibilityPublic)
          .modify(replaceMembers(Set(member)))
          .withActivities(activityEntities(stepPlanEntities().map(_.replaceCreators(List(member.person)))))
          .generateOne

        val nonPublicProject = renkuProjectEntities(visibilityNonPublic)
          .modify(replaceMembers(Set(member)))
          .withActivities(activityEntities(stepPlanEntities().map(_.replaceCreators(List(member.person)))))
          .generateOne

        for {
          _ <- provisionTestProjects(publicProject, nonPublicProject)
          results <- entitiesFinder.findEntities(
                       Criteria(Filters(roles = Set(member.role), visibilities = Set(projects.Visibility.Public)),
                                maybeUser = authUsers.generateOne.copy(id = memberId).some
                       )
                     )
        } yield results.results shouldMatchTo List(
          publicProject.to[model.Entity.Project].some,
          (publicProject.plans.head -> publicProject).to[model.Entity.Workflow].some,
          Option.when(member.role == projects.Role.Owner)(member.person.to[model.Entity.Person])
        ).flatten.sortBy(_.name)(nameOrdering)
      }

    "return project entities where the given user has one of the given roles" in projectsDSConfig.use { implicit pcc =>
      val memberId = personGitLabIds.generateOne
      val member   = projectMemberEntities(memberId.some).generateOne

      val roleProject1 = projects.Role.Owner
      val project1 = renkuProjectEntities(visibilityPublic)
        .modify(replaceMembers(Set(member.copy(role = roleProject1))))
        .generateOne
      val roleProject2 = projects.Role.Maintainer
      val project2 = renkuProjectEntities(visibilityPublic)
        .modify(replaceMembers(Set(member.copy(role = roleProject2))))
        .generateOne
      val roleProject3 = projects.Role.Reader
      val project3 = renkuProjectEntities(visibilityPublic)
        .modify(replaceMembers(Set(member.copy(role = roleProject3))))
        .generateOne

      for {
        _ <- provisionTestProjects(project1, project2, project3)
        results <- entitiesFinder.findEntities(
                     Criteria(
                       Filters(entityTypes = Set(EntityType.Project),
                               roles = Set(projects.Role.Maintainer, projects.Role.Reader)
                       ),
                       maybeUser = authUsers.generateOne.copy(id = memberId).some
                     )
                   )
      } yield results.results shouldMatchTo List(
        project2.to[model.Entity.Project],
        project3.to[model.Entity.Project]
      ).sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - with visibility filter" should {

    "return entities with matching visibility only" in projectsDSConfig.use { implicit pcc =>
      val publicProject = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      val internalProject = renkuProjectEntities(fixed(projects.Visibility.Internal))
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      val member = projectMemberEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val privateProject = renkuProjectEntities(fixed(projects.Visibility.Private))
        .modify(replaceMembers(to = Set(member)))
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      for {
        _ <- List(publicProject, internalProject, privateProject).traverse_(provisionTestProject)
        results <- entitiesFinder
                     .findEntities(
                       Criteria(
                         Filters(visibilities = Set(projects.Visibility.Public, projects.Visibility.Private)),
                         maybeUser = member.person.toAuthUser.some,
                         paging = PagingRequest(Page.first, PerPage(50))
                       )
                     )
      } yield results.results shouldBe allEntitiesFrom(publicProject)
        .addAllEntitiesFrom(privateProject)
        .addAllPersonsFrom(internalProject)
        .sortBy(_.name)(nameOrdering)
    }

    "return no entities when no match on visibility" in projectsDSConfig.use { implicit pcc =>
      val _ -> project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      List(project).traverse_(provisionTestProject) *>
        entitiesFinder
          .findEntities(Criteria(Filters(visibilities = visibilityNonPublic.generateSome.toSet)))
          .asserting(_.results shouldBe Nil)
    }
  }

  "findEntities - with namespace filter" should {

    "return entities with matching namespace only" in projectsDSConfig.use { implicit pcc =>
      val matchingProject = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      val nonMatchingProject = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified))
        .generateOne

      for {
        _ <- List(matchingProject, nonMatchingProject).traverse_(provisionTestProject)
        results <- entitiesFinder
                     .findEntities(
                       Criteria(
                         Filters(namespaces = Set(matchingProject.slug.toNamespace)),
                         paging = PagingRequest(Page.first, PerPage(50))
                       )
                     )
      } yield results.results shouldBe allEntitiesFrom(matchingProject)
        .removeAllPersons()
        .sortBy(_.name)(nameOrdering)
    }

    "return no namespace aware entities when no match on namespace" in projectsDSConfig.use { implicit pcc =>
      val _ -> project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()))
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne

      List(project).traverse_(provisionTestProject) *>
        entitiesFinder
          .findEntities(Criteria(Filters(namespaces = projectNamespaces.generateFixedSizeSet(1))))
          .asserting(_.results shouldBe Nil)
    }
  }

  "findEntities - with 'since' filter" should {

    "return entities with date >= 'since'" in projectsDSConfig.use { implicit pcc =>
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

      for {
        _       <- List(project).traverse_(provisionTestProject)
        results <- entitiesFinder.findEntities(Criteria(Filters(maybeSince = since.some)))
      } yield results.results shouldBe List(
        project.to[model.Entity.Project],
        (ds   -> project).to[model.Entity.Dataset],
        (plan -> project).to[model.Entity.Workflow]
      ).sortBy(_.name)(nameOrdering)
    }

    "return no entities with date < 'since'" in projectsDSConfig.use { implicit pcc =>
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

      List(project).traverse_(provisionTestProject) *>
        entitiesFinder.findEntities(Criteria(Filters(maybeSince = since.some))).asserting(_.results shouldBe Nil)
    }

    "return entities with date >= 'since' - case of DatePublished" in projectsDSConfig.use { implicit pcc =>
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

      for {
        _       <- List(dsProject).traverse_(provisionTestProject)
        results <- entitiesFinder.findEntities(Criteria(Filters(maybeSince = since.some)))
      } yield results.results shouldBe
        List((matchingDS -> dsProject).to[model.Entity.Dataset])
          .sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - with 'until' filter" should {

    "return entities with date <= 'until'" in projectsDSConfig.use { implicit pcc =>
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

      for {
        _       <- List(project).traverse_(provisionTestProject)
        results <- entitiesFinder.findEntities(Criteria(Filters(maybeUntil = until.some)))
      } yield results.results shouldBe List(
        project.to[model.Entity.Project],
        (ds   -> project).to[model.Entity.Dataset],
        (plan -> project).to[model.Entity.Workflow]
      ).sortBy(_.name)(nameOrdering)
    }

    "return no entities with date > 'until'" in projectsDSConfig.use { implicit pcc =>
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

      List(project).traverse_(provisionTestProject) *>
        entitiesFinder.findEntities(Criteria(Filters(maybeUntil = until.some))).asserting(_.results shouldBe Nil)
    }

    "return entities with date <= 'until' - case of DatePublished" in projectsDSConfig.use { implicit pcc =>
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

      for {
        _       <- List(dsProject).traverse_(provisionTestProject)
        results <- entitiesFinder.findEntities(Criteria(Filters(maybeUntil = until.some)))
      } yield results.results shouldBe List(
        (matchingDS -> dsProject).to[model.Entity.Dataset]
      ).sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - with both 'since' and 'until' filter" should {

    "return entities with date >= 'since' && date <= 'until'" in projectsDSConfig.use { implicit pcc =>
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

      for {
        _       <- List(project).traverse_(provisionTestProject)
        results <- entitiesFinder.findEntities(Criteria(Filters(maybeSince = since.some, maybeUntil = until.some)))
      } yield results.results shouldBe List(
        project.to[model.Entity.Project],
        (dsInternal -> project).to[model.Entity.Dataset],
        (dsExternal -> project).to[model.Entity.Dataset],
        (plan       -> project).to[model.Entity.Workflow]
      ).sortBy(_.name)(nameOrdering)
    }
  }

  "findEntities - with sorting" should {

    "be sorting by Name if requested in case-insensitive way" in projectsDSConfig.use { implicit pcc =>
      val commonPart = nonEmptyStrings().generateOne
      val project = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectName(projects.Name(s"a$commonPart")))
        .withActivities(activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceNonModified).modify(replaceDSName(datasets.Name(s"B$commonPart"))))
        .generateOne

      val direction = sortingDirections.generateOne
      for {
        _ <- List(project).traverse_(provisionTestProject)
        results <- entitiesFinder
                     .findEntities(Criteria(sorting = Sorting(Sort.By(Sort.ByName, direction))))
      } yield results.results shouldBe allEntitiesFrom(project).sortBy(_.name)(nameOrdering).use(direction)
    }

    "be sorting by Date if requested" in projectsDSConfig.use { implicit pcc =>
      val project = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(stepPlanEntities()), activityEntities(stepPlanEntities()))
        .withDatasets(datasetEntities(provenanceImportedExternal))
        .withDatasets(datasetEntities(provenanceInternal))
        .generateOne

      val direction = sortingDirections.generateOne
      for {
        _ <- List(project).traverse_(provisionTestProject)
        results <-
          entitiesFinder.findEntities(Criteria(sorting = Sorting(Sort.By(Sort.ByDate, direction)))).map(_.results)
      } yield {
        val expectedPersons = List.empty[model.Entity].addAllPersonsFrom(project).toSet

        val expectedSorted = List(project.to[model.Entity])
          .addAllDatasetsFrom(project)
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
    }

    "be sorting by Matching Score if requested" in projectsDSConfig.use { implicit pcc =>
      val query: NonBlank = "project score"

      val ds -> project = renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectName(to = projects.Name(query.value)))
        .withActivities(
          activityEntities(
            stepPlanEntities().map(
              _.replacePlanName(to = plans.Name(s"smth $query"))
                .replacePlanKeywords(to = Nil)
                .replacePlanDesc(to = None)
            )
          )
        )
        .addDataset(
          datasetEntities(provenanceNonModified)
            .modify(replaceDSSlug(to = sentenceContaining(query).generateAs(datasets.Slug)))
            .modify(replaceDSKeywords(to = Nil))
            .modify(replaceDSDesc(to = None))
        )
        .generateOne
      val plan :: Nil = project.plans

      val direction = sortingDirections.generateOne
      for {
        _ <- List(project).traverse_(provisionTestProject)
        results <- entitiesFinder
                     .findEntities(
                       Criteria(Filters(maybeQuery = Filters.Query(query.value).some),
                                Sorting(Sort.By(Sort.ByMatchingScore, direction))
                       )
                     )
      } yield results.resultsWithSkippedMatchingScore shouldBe List(
        project.to[model.Entity.Project], // should have the highest score as its name is the query
        (plan -> project).to[model.Entity.Workflow], // should have higher score as its name is the query with a prefix
        (ds   -> project).to[model.Entity.Dataset]
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

    "return the only page" in projectsDSConfig.use { implicit pcc =>
      val paging = PagingRequest(Page(1), PerPage(3))
      for {
        _ <- List(project).traverse_(provisionTestProject)
        results <-
          entitiesFinder
            .findEntities(
              Criteria(paging = paging, filters = Filters(entityTypes = Set(EntityType.Project, EntityType.Dataset)))
            )
      } yield {
        results.pagingInfo.pagingRequest shouldBe paging
        results.pagingInfo.total         shouldBe Total(3)
        results.results shouldBe List(project.to[model.Entity.Project])
          .addAllDatasetsFrom(project)
          .sortBy(_.name)(nameOrdering)
      }
    }

    "return the requested page with info if there are more" in projectsDSConfig.use { implicit pcc =>
      val paging = PagingRequest(Page(Random.nextInt(3) + 1), PerPage(1))
      for {
        _ <- List(project).traverse_(provisionTestProject)
        results <-
          entitiesFinder.findEntities(
            Criteria(paging = paging, filters = Filters(entityTypes = Set(EntityType.Project, EntityType.Dataset)))
          )
      } yield {
        results.pagingInfo.pagingRequest shouldBe paging
        results.pagingInfo.total         shouldBe Total(3)
        results.results shouldMatchTo List(project.to[model.Entity.Project])
          .addAllDatasetsFrom(project)
          .sortBy(_.name)(nameOrdering)
          .get(paging.page.value - 1)
          .toList
      }
    }

    "return no results if non-existing page requested" in projectsDSConfig.use { implicit pcc =>
      val paging = PagingRequest(Page(4), PerPage(1))
      for {
        _ <- List(project).traverse_(provisionTestProject)
        results <-
          entitiesFinder
            .findEntities(
              Criteria(paging = paging, filters = Filters(entityTypes = Set(EntityType.Project, EntityType.Dataset)))
            )
      } yield {
        results.pagingInfo.pagingRequest shouldBe paging
        results.pagingInfo.total         shouldBe Total(3)
        results.results                  shouldBe Nil
      }
    }
  }

  "findEntities - with images" should {

    "return images if present" in projectsDSConfig.use { implicit pcc =>
      val project = renkuProjectEntities(visibilityPublic)
        .suchThat(_.images.nonEmpty)
        .generateOne

      (provisionTestProject(project) *> entitiesFinder.findEntities(Criteria())).map { results =>
        val images = results.results.collect { case e: search.model.Entity.Project => e.images }.flatten
        images shouldBe project.images
      }
    }
  }

  "findEntities - security" should {

    val member = projectMemberEntities(personGitLabIds.toGeneratorOfSomes).generateOne

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

    "return public entities only if no auth user is given" in projectsDSConfig.use { implicit pcc =>
      for {
        _       <- List(privateProject, internalProject, publicProject).traverse_(provisionTestProject)
        results <- entitiesFinder.findEntities(Criteria())
      } yield results.results shouldBe List
        .empty[model.Entity]
        .addAllEntitiesFrom(publicProject)
        .addAllPersonsFrom(internalProject)
        .addAllPersonsFrom(privateProject)
        .sortBy(_.name)(nameOrdering)
    }

    "return public and internal entities only if auth user is given" in projectsDSConfig.use { implicit pcc =>
      for {
        _ <- List(privateProject, internalProject, publicProject).traverse_(provisionTestProject)
        results <- entitiesFinder
                     .findEntities(
                       Criteria(maybeUser =
                                  personEntities(personGitLabIds.toGeneratorOfSomes).generateSome.map(_.toAuthUser),
                                paging = PagingRequest.default.copy(perPage = PerPage(50))
                       )
                     )
      } yield results.results shouldBe List
        .empty[model.Entity]
        .addAllEntitiesFrom(publicProject)
        .addAllEntitiesFrom(internalProject)
        .addAllPersonsFrom(privateProject)
        .sortBy(_.name)(nameOrdering)
    }

    "return any visibility entities if the given auth user has access to them" in projectsDSConfig.use { implicit pcc =>
      for {
        _ <- List(privateProject, internalProject, publicProject).traverse_(provisionTestProject)
        results <- entitiesFinder
                     .findEntities(
                       Criteria(maybeUser = member.person.toAuthUser.some,
                                paging = PagingRequest.default.copy(perPage = PerPage(50))
                       )
                     )
      } yield results.results shouldBe List
        .empty[model.Entity]
        .addAllEntitiesFrom(publicProject)
        .addAllEntitiesFrom(internalProject)
        .addAllEntitiesFrom(privateProject)
        .sortBy(_.name)(nameOrdering)
    }

    "not return private projects/datasets of different user" in projectsDSConfig.use { implicit pcc =>
      val otherMember = personEntities(
        personGitLabIds.suchThat(id => !member.person.maybeGitLabId.contains(id)).toGeneratorOfSomes
      ).generateOne
      for {
        _ <- provisionTestProjects(privateProject, internalProject, publicProject)
        results <- entitiesFinder
                     .findEntities(
                       Criteria(maybeUser = otherMember.toAuthUser.some,
                                paging = PagingRequest.default.copy(perPage = PerPage(50))
                       )
                     )
      } yield results.results shouldBe List
        .empty[model.Entity]
        .addAllEntitiesFrom(publicProject)
        .addAllEntitiesFrom(internalProject)
        .addAllPersonsFrom(privateProject)
        .sortBy(_.name)(nameOrdering)
    }
  }

  private def nameOrSlugContains(word: String)(entity: Entity): Boolean =
    entity.name.value.contains(word) || (entity match {
      case e: Entity.Dataset => e.slug.value.contains(word)
      case _ => false
    })
}
