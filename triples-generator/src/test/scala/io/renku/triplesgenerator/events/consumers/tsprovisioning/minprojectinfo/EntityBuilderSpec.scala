/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers
package tsprovisioning
package minprojectinfo

import CategoryGenerators._
import cats.data.EitherT
import cats.syntax.all._
import io.renku.events.consumers
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model._
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.testentities._
import io.renku.http.client.AccessToken
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import projectinfo.ProjectInfoFinder
import projects._

import scala.language.reflectiveCalls
import scala.util.{Failure, Try}

class EntityBuilderSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "buildEntity" should {

    "find project info in GitLab and build the Project entity based on the info" in new TestCase {
      val projectInfo = gitLabProjectInfos.generateOne

      givenFindProjectInfo(projectInfo.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](projectInfo.some))

      entityBuilder
        .buildEntity(MinProjectInfoEvent(consumers.Project(projectInfo.id, projectInfo.path)))
        .value shouldBe projectInfo.to[entities.Project].asRight.pure[Try]
    }

    "fail if there's no project info found for the project" in new TestCase {
      val event = minProjectInfoEvents.generateOne

      givenFindProjectInfo(event.project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](Option.empty[GitLabProjectInfo]))

      val Failure(error) = entityBuilder.buildEntity(event).value

      error            shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
      error.getMessage shouldBe show"${event.project} not found in GitLab"
    }

    "fail if fetching the project info fails" in new TestCase {
      val event = minProjectInfoEvents.generateOne

      val exception = exceptions.generateOne
      givenFindProjectInfo(event.project.path)
        .returning(EitherT(exception.raiseError[Try, Either[ProcessingRecoverableError, Option[GitLabProjectInfo]]]))

      entityBuilder.buildEntity(event).value shouldBe Failure(exception)
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val projectInfoFinder = mock[ProjectInfoFinder[Try]]
    val entityBuilder     = new EntityBuilderImpl[Try](projectInfoFinder, renkuUrl)

    def givenFindProjectInfo(projectPath: projects.Path) = new {
      def returning(result: EitherT[Try, ProcessingRecoverableError, Option[GitLabProjectInfo]]) =
        (projectInfoFinder
          .findProjectInfo(_: projects.Path)(_: Option[AccessToken]))
          .expects(projectPath, maybeAccessToken)
          .returning(result)
    }
  }

  private lazy val gitLabProjectInfos: Gen[GitLabProjectInfo] = for {
    id              <- projectIds
    name            <- projectNames
    path            <- projectPaths
    dateCreated     <- projectCreatedDates()
    maybeDesc       <- projectDescriptions.toGeneratorOfOptions
    maybeCreator    <- projectMembers.toGeneratorOfOptions
    keywords        <- projectKeywords.toGeneratorOfSet()
    members         <- projectMembers.toGeneratorOfSet()
    visibility      <- projectVisibilities
    maybeParentPath <- projectPaths.toGeneratorOfOptions
  } yield GitLabProjectInfo(id,
                            name,
                            path,
                            dateCreated,
                            maybeDesc,
                            maybeCreator,
                            keywords,
                            members,
                            visibility,
                            maybeParentPath
  )

  private implicit class ProjectInfoOps(projectInfo: GitLabProjectInfo) {
    def to[T](implicit convert: GitLabProjectInfo => T): T = convert(projectInfo)
  }

  private implicit val toEntitiesProject: GitLabProjectInfo => entities.Project = {
    case GitLabProjectInfo(_,
                           name,
                           path,
                           dateCreated,
                           maybeDescription,
                           maybeCreator,
                           keywords,
                           members,
                           visibility,
                           Some(parentPath)
        ) =>
      entities.NonRenkuProject.WithParent(ResourceId(path),
                                          path,
                                          name,
                                          maybeDescription,
                                          dateCreated,
                                          maybeCreator.map(toPerson),
                                          visibility,
                                          keywords,
                                          members.map(toPerson),
                                          ResourceId(parentPath)
      )
    case GitLabProjectInfo(_,
                           name,
                           path,
                           dateCreated,
                           maybeDescription,
                           maybeCreator,
                           keywords,
                           members,
                           visibility,
                           None
        ) =>
      entities.NonRenkuProject.WithoutParent(ResourceId(path),
                                             path,
                                             name,
                                             maybeDescription,
                                             dateCreated,
                                             maybeCreator.map(toPerson),
                                             visibility,
                                             keywords,
                                             members.map(toPerson)
      )
  }

  private def toPerson(projectMember: ProjectMember)(implicit renkuUrl: RenkuUrl): entities.Person =
    projectMember match {
      case ProjectMemberNoEmail(name, _, gitLabId) =>
        entities.Person.WithGitLabId(persons.ResourceId(gitLabId),
                                     gitLabId,
                                     name,
                                     maybeEmail = None,
                                     maybeOrcidId = None,
                                     maybeAffiliation = None
        )
      case ProjectMemberWithEmail(name, _, gitLabId, email) =>
        entities.Person.WithGitLabId(persons.ResourceId(gitLabId),
                                     gitLabId,
                                     name,
                                     email.some,
                                     maybeOrcidId = None,
                                     maybeAffiliation = None
        )
    }
}
