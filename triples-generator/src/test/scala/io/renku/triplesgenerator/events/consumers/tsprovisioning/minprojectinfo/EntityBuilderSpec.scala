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
import io.renku.graph.model._
import io.renku.graph.model.gitlab.{GitLabMember, GitLabProjectInfo, GitLabUser}
import io.renku.graph.model.testentities.ModelOps
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.http.client.AccessToken
import io.renku.jsonld.syntax._
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import projectinfo.ProjectInfoFinder
import projects._

import scala.language.reflectiveCalls
import scala.util.Try

class EntityBuilderSpec
    extends AnyWordSpec
    with MockFactory
    with should.Matchers
    with TryValues
    with EntitiesGenerators
    with ModelOps {

  "buildEntity" should {

    "find project info in GitLab and build the Project entity based on the info" in new TestCase {

      val projectInfo = gitLabProjectInfos.generateOne

      givenFindProjectInfo(projectInfo.slug)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](projectInfo.some))

      entityBuilder
        .buildEntity(MinProjectInfoEvent(consumers.Project(projectInfo.id, projectInfo.slug)))
        .value
        .success
        .value shouldBe projectInfo.to[entities.Project].asRight
    }

    "fail if there's no project info found for the project" in new TestCase {

      val event = minProjectInfoEvents.generateOne

      givenFindProjectInfo(event.project.slug)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](Option.empty[GitLabProjectInfo]))

      val error = entityBuilder.buildEntity(event).value.failure.exception

      error            shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
      error.getMessage shouldBe show"${event.project} not found in GitLab"
    }

    "fail if fetching the project info fails" in new TestCase {

      val event = minProjectInfoEvents.generateOne

      val exception = exceptions.generateOne
      givenFindProjectInfo(event.project.slug)
        .returning(EitherT(exception.raiseError[Try, Either[ProcessingRecoverableError, Option[GitLabProjectInfo]]]))

      entityBuilder.buildEntity(event).value.failure.exception shouldBe exception
    }

    "fail if converting GL info to Project fails" in new TestCase {

      val projectInfo = {
        val gl = gitLabProjectInfos.generateOne
        gl.copy(dateModified = timestamps(max = gl.dateCreated.value.minusSeconds(1)).generateAs(projects.DateModified))
      }
      givenFindProjectInfo(projectInfo.slug)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](projectInfo.some))

      entityBuilder
        .buildEntity(MinProjectInfoEvent(consumers.Project(projectInfo.id, projectInfo.slug)))
        .value
        .failure
        .exception shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    private val projectInfoFinder = mock[ProjectInfoFinder[Try]]
    val entityBuilder             = new EntityBuilderImpl[Try](projectInfoFinder)

    def givenFindProjectInfo(projectSlug: projects.Slug) = new {
      def returning(result: EitherT[Try, ProcessingRecoverableError, Option[GitLabProjectInfo]]) =
        (projectInfoFinder
          .findProjectInfo(_: projects.Slug)(_: Option[AccessToken]))
          .expects(projectSlug, maybeAccessToken)
          .returning(result)
    }
  }

  private implicit class ProjectInfoOps(projectInfo: GitLabProjectInfo) {
    def to[T](implicit convert: GitLabProjectInfo => T): T = convert(projectInfo)
  }

  private implicit val toEntitiesProject: GitLabProjectInfo => entities.Project = {
    case GitLabProjectInfo(_,
                           name,
                           slug,
                           dateCreated,
                           dateModified,
                           maybeDescription,
                           maybeCreator,
                           keywords,
                           members,
                           visibility,
                           Some(parentSlug),
                           avatarUrl
        ) =>
      entities.NonRenkuProject.WithParent(
        ResourceId(slug),
        slug,
        name,
        maybeDescription,
        dateCreated,
        dateModified,
        maybeCreator.map(toPerson),
        visibility,
        keywords,
        members.map(toMember),
        ResourceId(parentSlug),
        convertImageUris(ResourceId(slug).asEntityId)(avatarUrl.toList)
      )
    case GitLabProjectInfo(_,
                           name,
                           slug,
                           dateCreated,
                           dateModified,
                           maybeDescription,
                           maybeCreator,
                           keywords,
                           members,
                           visibility,
                           None,
                           avatarUrl
        ) =>
      entities.NonRenkuProject.WithoutParent(
        ResourceId(slug),
        slug,
        name,
        maybeDescription,
        dateCreated,
        dateModified,
        maybeCreator.map(toPerson),
        visibility,
        keywords,
        members.map(toMember),
        convertImageUris(ResourceId(slug).asEntityId)(avatarUrl.toList)
      )
  }

  private def toMember(member: GitLabMember)(implicit renkuUrl: RenkuUrl): entities.Project.Member =
    entities.Project.Member(toPerson(member.user), member.role)

  private def toPerson(user: GitLabUser)(implicit renkuUrl: RenkuUrl): entities.Person =
    entities.Person.WithGitLabId(
      persons.ResourceId(user.gitLabId),
      user.gitLabId,
      user.name,
      maybeEmail = user.email,
      maybeOrcidId = None,
      maybeAffiliation = None
    )
}
