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

package io.renku.graph.model.entities

import cats.data.{Validated, ValidatedNel}
import cats.syntax.all._
import io.renku.cli.model.{CliPerson, CliProject}
import io.renku.graph.model._
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.entities.ProjectLens._
import io.renku.graph.model.images.Image
import io.renku.graph.model.projects.{DateCreated, DateModified, Description, Keyword, ResourceId}
import io.renku.graph.model.versions.{CliVersion, SchemaVersion}

private[entities] object CliProjectConverter {

  def fromCli(cliProject: CliProject, allPersons: Set[CliPerson], gitLabInfo: GitLabProjectInfo)(implicit
      renkuUrl: RenkuUrl
  ): ValidatedNel[String, Project] = {
    val creatorV = cliProject.creator.traverse(Person.fromCli)
    val planV = cliProject.plans.traverse(
      _.fold(StepPlan.fromCli,
             CompositePlan.fromCli,
             p => StepPlan.fromCli(p.asCliStepPlan),
             p => CompositePlan.fromCli(p.asCliCompositePlan)
      )
    )
    val dependencyLinks = planV.map(new DecodingDependencyLinks(_))
    val datasetV        = cliProject.datasets.traverse(Dataset.fromCli)
    val activityV       = dependencyLinks.andThen(links => cliProject.activities.traverse(Activity.fromCli(_, links)))
    val allPersonV      = allPersons.toList.traverse(Person.fromCli)
    val descr           = cliProject.description.orElse(gitLabInfo.maybeDescription)
    val keywords = cliProject.keywords match {
      case s if s.isEmpty => gitLabInfo.keywords
      case s              => s
    }
    val dateCreated  = (gitLabInfo.dateCreated :: cliProject.dateCreated :: Nil).min
    val dateModified = (gitLabInfo.dateModified :: cliProject.dateModified :: Nil).max
    val gitlabImage  = gitLabInfo.avatarUrl.map(Image.projectImage(ResourceId(gitLabInfo.slug), _))
    val all          = (creatorV, allPersonV, datasetV, activityV, planV).mapN(Tuple5.apply)
    all.andThen { case (creator, persons, datasets, activities, plans) =>
      newProject(
        gitLabInfo,
        dateCreated,
        dateModified,
        descr,
        cliProject.agentVersion,
        keywords,
        cliProject.schemaVersion,
        persons.toSet ++ creator.toSet,
        activities.sortBy(_.startTime),
        datasets,
        plans,
        cliProject.images match {
          case Nil  => gitlabImage.toList
          case imgs => imgs
        }
      )
    }
  }

  private class DecodingDependencyLinks(allPlans: List[Plan]) extends DependencyLinks {
    override def findStepPlan(planId: plans.ResourceId): Option[StepPlan] =
      collectStepPlans(allPlans).find(_.resourceId == planId)
  }

  private def newProject(gitLabInfo:       GitLabProjectInfo,
                         dateCreated:      DateCreated,
                         dateModified:     DateModified,
                         maybeDescription: Option[Description],
                         maybeAgent:       Option[CliVersion],
                         keywords:         Set[Keyword],
                         maybeVersion:     Option[SchemaVersion],
                         allJsonLdPersons: Set[Person],
                         activities:       List[Activity],
                         datasets:         List[Dataset[Dataset.Provenance]],
                         plans:            List[Plan],
                         images:           List[Image]
  )(implicit renkuUrl: RenkuUrl): ValidatedNel[String, Project] =
    (maybeAgent, maybeVersion, gitLabInfo.maybeParentSlug) match {
      case (Some(agent), Some(version), Some(parentSlug)) =>
        RenkuProject.WithParent
          .from(
            ResourceId(gitLabInfo.slug),
            gitLabInfo.slug,
            gitLabInfo.name,
            maybeDescription,
            agent,
            dateCreated,
            dateModified,
            maybeCreator(allJsonLdPersons)(gitLabInfo),
            gitLabInfo.visibility,
            keywords,
            members(allJsonLdPersons)(gitLabInfo),
            version,
            activities,
            datasets,
            plans,
            parentResourceId = ResourceId(parentSlug),
            images
          )
          .widen[Project]
      case (Some(agent), Some(version), None) =>
        RenkuProject.WithoutParent
          .from(
            ResourceId(gitLabInfo.slug),
            gitLabInfo.slug,
            gitLabInfo.name,
            maybeDescription,
            agent,
            dateCreated,
            dateModified,
            maybeCreator(allJsonLdPersons)(gitLabInfo),
            gitLabInfo.visibility,
            keywords,
            members(allJsonLdPersons)(gitLabInfo),
            version,
            activities,
            datasets,
            plans,
            images
          )
          .widen[Project]
      case (None, None, Some(parentSlug)) =>
        NonRenkuProject.WithParent
          .from(
            ResourceId(gitLabInfo.slug),
            gitLabInfo.slug,
            gitLabInfo.name,
            maybeDescription,
            dateCreated,
            dateModified,
            maybeCreator(allJsonLdPersons)(gitLabInfo),
            gitLabInfo.visibility,
            keywords,
            members(allJsonLdPersons)(gitLabInfo),
            parentResourceId = ResourceId(parentSlug),
            images
          )
          .widen[Project]
      case (None, None, None) =>
        NonRenkuProject.WithoutParent
          .from(
            ResourceId(gitLabInfo.slug),
            gitLabInfo.slug,
            gitLabInfo.name,
            maybeDescription,
            dateCreated,
            dateModified,
            maybeCreator(allJsonLdPersons)(gitLabInfo),
            gitLabInfo.visibility,
            keywords,
            members(allJsonLdPersons)(gitLabInfo),
            images
          )
          .widen[Project]
      case (maybeAgent, maybeVersion, maybeParent) =>
        Validated.invalidNel[String, Project](
          s"Invalid project data " +
            s"agent: $maybeAgent, " +
            s"schemaVersion: $maybeVersion, " +
            s"parent: $maybeParent"
        )
    }

  private def maybeCreator(
      allJsonLdPersons: Set[Person]
  )(gitLabInfo: GitLabProjectInfo)(implicit renkuUrl: RenkuUrl): Option[Person] =
    gitLabInfo.maybeCreator.map { creator =>
      allJsonLdPersons
        .find(byEmailOrUsername(creator))
        .map(merge(creator).andThen(_.person))
        .getOrElse(toMember(creator).person)
    }

  private def members(
      allJsonLdPersons: Set[Person]
  )(gitLabInfo: GitLabProjectInfo)(implicit renkuUrl: RenkuUrl): Set[Project.Member] =
    gitLabInfo.members.map(member =>
      allJsonLdPersons
        .find(byEmailOrUsername(member))
        .map(merge(member))
        .getOrElse(toMember(member))
    )

  private lazy val byEmailOrUsername: ProjectMember => Person => Boolean = {
    case member: ProjectMemberWithEmail =>
      person =>
        person.maybeEmail match {
          case Some(personEmail) => personEmail == member.email
          case None              => person.name.value == member.username.value
        }
    case member: ProjectMemberNoEmail => person => person.name.value == member.username.value
  }

  private def merge(member: Project.ProjectMember)(implicit renkuUrl: RenkuUrl): Person => Project.Member =
    member match {
      case ProjectMemberWithEmail(name, _, gitLabId, email, _) =>
        p =>
          Project.Member(
            p.add(gitLabId).copy(name = name, maybeEmail = email.some),
            member.role
          )
      case ProjectMemberNoEmail(name, _, gitLabId, _) =>
        p =>
          Project.Member(
            p.add(gitLabId).copy(name = name),
            member.role
          )
    }

  private def toMember(projectMember: ProjectMember)(implicit renkuUrl: RenkuUrl): Project.Member =
    projectMember match {
      case ProjectMemberNoEmail(name, _, gitLabId, _) =>
        Project.Member(
          Person.WithGitLabId(
            persons.ResourceId(gitLabId),
            gitLabId,
            name,
            maybeEmail = None,
            maybeOrcidId = None,
            maybeAffiliation = None
          ),
          projectMember.role
        )
      case ProjectMemberWithEmail(name, _, gitLabId, email, _) =>
        Project.Member(
          Person.WithGitLabId(
            persons.ResourceId(gitLabId),
            gitLabId,
            name,
            email.some,
            maybeOrcidId = None,
            maybeAffiliation = None
          ),
          projectMember.role
        )
    }
}
