/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.MonadError
import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{DateCreated, Path, ResourceId}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.Email
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError._
import ch.datascience.triplesgenerator.eventprocessing.EventProcessingGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.UpdateFunction
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators.curationUpdateFunctions
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.IOTriplesCurator.CurationRecoverableError
import eu.timepit.refined.auto._
import org.scalamock.handlers.CallHandler
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.{higherKinds, reflectiveCalls}

class UpdateFunctionsCreatorSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "create" should {

    "do nothing if both projects from GitLab and KG have no forks" in new TestCase {

      given(gitLabProjects(maybeParentPaths   = emptyOptionOf[Path]).generateOne).existsInGitLab
      given(kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne).existsInKG

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List.empty[UpdateFunction[IO]]
      )
    }

    "do nothing if no projects in GitLab and in KG" in new TestCase {

      given(gitLabProjects(maybeParentPaths   = emptyOptionOf[Path]).generateOne).doesNotExistsInGitLab
      given(kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne).doesNotExistsInKG

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List.empty[UpdateFunction[IO]]
      )
    }

    "do nothing if no projects in GitLab" in new TestCase {

      given(gitLabProjects(maybeParentPaths   = emptyOptionOf[Path]).generateOne).doesNotExistsInGitLab
      given(kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne).existsInKG

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List.empty[UpdateFunction[IO]]
      )
    }

    "do nothing if no projects in KG" in new TestCase {

      given(gitLabProjects(maybeParentPaths   = emptyOptionOf[Path]).generateOne).existsInGitLab
      given(kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne).doesNotExistsInKG

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List.empty[UpdateFunction[IO]]
      )
    }

    Set(
      BadRequestException(nonBlankStrings().generateOne),
      MappingException(nonBlankStrings().generateOne, exceptions.generateOne),
      UnauthorizedException
    ) foreach { exception =>
      s"fail if finding GitLab project fails with ${exception.getClass}" in new TestCase {

        given(kgProjects().generateOne).existsInKG

        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(exception.raiseError[IO, Option[GitLabProject]])

        intercept[Exception] {
          updater.create(event).value.unsafeRunSync()
        } shouldBe exception
      }

      s"fail if finding KG project fails with ${exception.getClass}" in new TestCase {

        given(gitLabProjects(maybeParentPaths = emptyOptionOf[Path]).generateOne).existsInGitLab

        (kgInfoFinder
          .findProject(_: Path))
          .expects(event.project.path)
          .returning(exception.raiseError[IO, Option[KGProject]])

        intercept[Exception] {
          updater.create(event).value.unsafeRunSync()
        } shouldBe exception
      }
    }

    Set(
      UnexpectedResponseException(nonBlankStrings().generateOne),
      ConnectivityException(nonBlankStrings().generateOne, exceptions.generateOne)
    ) foreach { exception =>
      s"return $CurationRecoverableError if finding GitLab project fails with ${exception.getClass.getSimpleName}" in new TestCase {

        given(kgProjects().generateOne).existsInKG

        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(exception.raiseError[IO, Option[GitLabProject]])

        updater.create(event).value.unsafeRunSync() shouldBe Left {
          CurationRecoverableError("Problem with finding fork info", exception)
        }
      }

      s"return $CurationRecoverableError if finding KG project fails ${exception.getClass.getSimpleName}" in new TestCase {

        given(gitLabProjects(maybeParentPaths = emptyOptionOf[Path]).generateOne).existsInGitLab

        (kgInfoFinder
          .findProject(_: Path))
          .expects(event.project.path)
          .returning(exception.raiseError[IO, Option[KGProject]])

        updater.create(event).value.unsafeRunSync() shouldBe Left {
          CurationRecoverableError("Problem with finding fork info", exception)
        }
      }
    }
  }

  "updateForkInfo - cases when forks in KG and in GitLab" should {

    "do nothing if forks are the same" in new TestCase {

      val commonFork = projectPaths.generateOne
      given(gitLabProjects(commonFork).generateOne).existsInGitLab
      given(kgProjects(commonFork).generateOne).existsInKG

      updater.create(event).value.unsafeRunSync() shouldBe Right(List.empty[UpdateFunction[IO]])
    }

    "recreate wasDerivedFrom " +
      "if forks are different " +
      "but emails and dateCreated are the same" in new TestCase {

      val forkInGitLab        = projectPaths.generateOne
      val commonEmail         = userEmails.generateSome
      val dateCreatedInGitLab = projectCreatedDates.generateOne

      given {
        gitLabProjects(forkInGitLab).generateOne.copy(
          maybeCreator = gitLabCreator(commonEmail).generateSome,
          dateCreated  = dateCreatedInGitLab
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(projectResourceIds.toGeneratorOfSomes).generateOne.copy(
          maybeCreator = kgCreator(commonEmail).generateSome,
          dateCreated  = dateCreatedInGitLab
        )
      }.existsInKG

      val wasDerivedFromRecreate = (updatesCreator
        .recreateWasDerivedFrom[IO](_: ResourceId, _: Path)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, forkInGitLab, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        wasDerivedFromRecreate
      )
    }

    "recreate wasDerivedFrom " +
      "if forks are different " +
      "but user names and dateCreated are the same in the absence of emails" in new TestCase {

      val forkInGitLab        = projectPaths.generateOne
      val commonUserName      = userNames.generateOne
      val dateCreatedInGitLab = projectCreatedDates.generateOne

      val gitLabProject = given {
        gitLabProjects(forkInGitLab).generateOne.copy(
          maybeCreator = gitLabCreator(maybeEmail = None, maybeName = commonUserName.some).generateSome,
          dateCreated  = dateCreatedInGitLab
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(projectResourceIds.toGeneratorOfSomes).generateOne.copy(
          maybeCreator = kgCreator(maybeEmail = None, name = commonUserName).generateSome,
          dateCreated  = dateCreatedInGitLab
        )
      }.existsInKG

      val wasDerivedFromRecreate = (updatesCreator
        .recreateWasDerivedFrom[IO](_: ResourceId, _: Path)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, forkInGitLab, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        wasDerivedFromRecreate
      )
    }

    "recreate wasDerivedFrom and dateCreated and link a new creator " +
      "if many fields are different" +
      "and there is a Person with the new email already in the KG" in new TestCase {

      val forkInGitLab  = projectPaths.generateOne
      val emailInGitLab = userEmails.generateOne
      val gitLabProject = given {
        gitLabProjects(forkInGitLab).generateOne.copy(
          maybeCreator = gitLabCreator(Some(emailInGitLab)).generateSome
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(projectResourceIds.toGeneratorOfSomes).generateOne.copy(
          maybeCreator = kgCreator(userEmails.generateSome).generateSome
        )
      }.existsInKG

      val wasDerivedFromRecreate = (updatesCreator
        .recreateWasDerivedFrom[IO](_: ResourceId, _: Path)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, forkInGitLab, *)
        .returningUpdates

      val newCreatorId = userResourceIds(Some(emailInGitLab)).generateOne
      given(
        newCreatorId = Some(newCreatorId),
        forEmail     = emailInGitLab
      ).existsInKG
      val creatorUpdates = (updatesCreator
        .swapCreator[IO](_: ResourceId, _: users.ResourceId)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, newCreatorId, *)
        .returningUpdates

      val recreateDateCreated = (updatesCreator
        .recreateDateCreated[IO](_: ResourceId, _: DateCreated)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, gitLabProject.dateCreated, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List(
          wasDerivedFromRecreate,
          creatorUpdates,
          recreateDateCreated
        ).flatten
      )
    }

    "recreate wasDerivedFrom and dateCreated and create and link a new creator " +
      "if many fields are different " +
      "and there is no Person with the new email in the KG" in new TestCase {

      val forkInGitLab  = projectPaths.generateOne
      val emailInGitLab = userEmails.generateOne
      val gitLabProject = given {
        gitLabProjects(forkInGitLab).generateOne.copy(
          maybeCreator = gitLabCreator(Some(emailInGitLab)).generateSome
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(projectResourceIds.toGeneratorOfSomes).generateOne.copy(
          maybeCreator = kgCreator(userEmails.generateSome).generateSome
        )
      }.existsInKG

      val wasDerivedFromRecreate = (updatesCreator
        .recreateWasDerivedFrom[IO](_: ResourceId, _: Path)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, forkInGitLab, *)
        .returningUpdates

      given(newCreatorId = None, forEmail = emailInGitLab).existsInKG
      val creatorUpdates = (updatesCreator
        .addNewCreator[IO](_: ResourceId, _: Option[Email], _: Option[users.Name])(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId,
                 gitLabProject.maybeCreator.flatMap(_.maybeEmail),
                 gitLabProject.maybeCreator.flatMap(_.maybeName),
                 *)
        .returningUpdates

      val recreateDateCreated = (updatesCreator
        .recreateDateCreated[IO](_: ResourceId, _: DateCreated)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, gitLabProject.dateCreated, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List(
          wasDerivedFromRecreate,
          creatorUpdates,
          recreateDateCreated
        ).flatten
      )
    }

    "recreate wasDerivedFrom and dateCreated and create and link a new creator " +
      "if many fields are different " +
      "and there's only creator username in GitLab" in new TestCase {

      val forkInGitLab = projectPaths.generateOne
      val gitLabProject = given {
        gitLabProjects(forkInGitLab).generateOne.copy(
          maybeCreator = gitLabCreator(maybeEmail = None).generateSome.map(_.copy(maybeName = userNames.generateSome))
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(projectResourceIds.toGeneratorOfSomes).generateOne
      }.existsInKG

      val wasDerivedFromRecreate = (updatesCreator
        .recreateWasDerivedFrom[IO](_: ResourceId, _: Path)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, forkInGitLab, *)
        .returningUpdates

      val creatorUpdates = (updatesCreator
        .addNewCreator[IO](_: ResourceId, _: Option[Email], _: Option[users.Name])(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId,
                 gitLabProject.maybeCreator.flatMap(_.maybeEmail),
                 gitLabProject.maybeCreator.flatMap(_.maybeName),
                 *)
        .returningUpdates

      val recreateDateCreated = (updatesCreator
        .recreateDateCreated[IO](_: ResourceId, _: DateCreated)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, gitLabProject.dateCreated, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List(
          wasDerivedFromRecreate,
          creatorUpdates,
          recreateDateCreated
        ).flatten
      )
    }

    "recreate wasDerivedFrom and dateCreated and only unlink creator " +
      "if many fields are different " +
      "and email and name do not exist in GitLab" in new TestCase {

      val forkInGitLab = projectPaths.generateOne
      val gitLabProject = given {
        gitLabProjects(forkInGitLab).generateOne.copy(
          maybeCreator = gitLabCreator(maybeEmail = None).generateSome.map(_.copy(maybeName = None))
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(projectResourceIds.toGeneratorOfSomes).generateOne
      }.existsInKG

      val wasDerivedFromRecreate = (updatesCreator
        .recreateWasDerivedFrom[IO](_: ResourceId, _: Path)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, forkInGitLab, *)
        .returningUpdates

      val creatorUpdates = (updatesCreator
        .addNewCreator[IO](_: ResourceId, _: Option[Email], _: Option[users.Name])(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId,
                 gitLabProject.maybeCreator.flatMap(_.maybeEmail),
                 gitLabProject.maybeCreator.flatMap(_.maybeName),
                 *)
        .returningUpdates

      val recreateDateCreated = (updatesCreator
        .recreateDateCreated[IO](_: ResourceId, _: DateCreated)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, gitLabProject.dateCreated, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List(
          wasDerivedFromRecreate,
          creatorUpdates,
          recreateDateCreated
        ).flatten
      )

    }

    "fail if finding Person with an email in the KG fails" in new TestCase {

      val emailInGitLab = userEmails.generateOne
      given {
        gitLabProjects(projectPaths.generateOne).generateOne.copy(
          maybeCreator = gitLabCreator(Some(emailInGitLab)).generateSome
        )
      }.existsInGitLab

      given {
        kgProjects(projectResourceIds.toGeneratorOfSomes).generateOne.copy(
          maybeCreator = kgCreator(userEmails.generateSome).generateSome
        )
      }.existsInKG

      val exception = exceptions.generateOne
      (kgInfoFinder
        .findCreatorId(_: Email))
        .expects(emailInGitLab)
        .returning(exception.raiseError[IO, Option[users.ResourceId]])

      intercept[Exception] {
        updater.create(event).value.unsafeRunSync()
      } shouldBe exception
    }
  }

  "updateForkInfo - cases when fork only in GitLab" should {

    "create wasDerivedFrom and dateCreated and link a new creator " +
      "if there's a Person with the new email already in the KG" in new TestCase {

      val forkInGitLab  = projectPaths.generateOne
      val emailInGitLab = userEmails.generateOne
      val gitLabProject = given {
        gitLabProjects(forkInGitLab).generateOne.copy(
          maybeCreator = gitLabCreator(Some(emailInGitLab)).generateSome
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne
      }.existsInKG

      val wasDerivedFromInsert = (updatesCreator
        .insertWasDerivedFrom[IO](_: ResourceId, _: Path)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, forkInGitLab, *)
        .returningUpdates

      val newCreatorId = userResourceIds(Some(emailInGitLab)).generateOne
      given(
        newCreatorId = Some(newCreatorId),
        forEmail     = emailInGitLab
      ).existsInKG
      val creatorUpdates = (updatesCreator
        .swapCreator[IO](_: ResourceId, _: users.ResourceId)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, newCreatorId, *)
        .returningUpdates

      val recreateDateCreated = (updatesCreator
        .recreateDateCreated[IO](_: ResourceId, _: DateCreated)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, gitLabProject.dateCreated, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List(
          wasDerivedFromInsert,
          creatorUpdates,
          recreateDateCreated
        ).flatten
      )
    }

    "create wasDerivedFrom, dateCreated and creator " +
      "and link the new creator to the project in KG " +
      "if there is no Person with the new email in the KG yet" in new TestCase {

      val forkInGitLab  = projectPaths.generateOne
      val emailInGitLab = userEmails.generateOne
      val gitLabProject = given {
        gitLabProjects(forkInGitLab).generateOne.copy(
          maybeCreator = gitLabCreator(Some(emailInGitLab)).generateSome
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne
      }.existsInKG

      val wasDerivedFromInsert = (updatesCreator
        .insertWasDerivedFrom[IO](_: ResourceId, _: Path)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, forkInGitLab, *)
        .returningUpdates

      given(newCreatorId = None, forEmail = emailInGitLab).existsInKG
      val creatorUpdates = (updatesCreator
        .addNewCreator[IO](_: ResourceId, _: Option[Email], _: Option[users.Name])(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId,
                 gitLabProject.maybeCreator.flatMap(_.maybeEmail),
                 gitLabProject.maybeCreator.flatMap(_.maybeName),
                 *)
        .returningUpdates

      val recreateDateCreated = (updatesCreator
        .recreateDateCreated[IO](_: ResourceId, _: DateCreated)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, gitLabProject.dateCreated, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List(
          wasDerivedFromInsert,
          creatorUpdates,
          recreateDateCreated
        ).flatten
      )
    }

    "create wasDerivedFrom, dateCreated and creator " +
      "and link the new creator to the project in KG " +
      "if there's only creator username in GitLab" in new TestCase {

      val forkInGitLab = projectPaths.generateOne
      val gitLabProject = given {
        gitLabProjects(forkInGitLab).generateOne.copy(
          maybeCreator = gitLabCreator(maybeEmail = None).generateSome.map(_.copy(maybeName = userNames.generateSome))
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne
      }.existsInKG

      val wasDerivedFromInsert = (updatesCreator
        .insertWasDerivedFrom[IO](_: ResourceId, _: Path)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, forkInGitLab, *)
        .returningUpdates

      val creatorUpdates = (updatesCreator
        .addNewCreator[IO](_: ResourceId, _: Option[Email], _: Option[users.Name])(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId,
                 gitLabProject.maybeCreator.flatMap(_.maybeEmail),
                 gitLabProject.maybeCreator.flatMap(_.maybeName),
                 *)
        .returningUpdates

      val recreateDateCreated = (updatesCreator
        .recreateDateCreated[IO](_: ResourceId, _: DateCreated)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, gitLabProject.dateCreated, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List(
          wasDerivedFromInsert,
          creatorUpdates,
          recreateDateCreated
        ).flatten
      )
    }

    "create wasDerivedFrom and dateCreated and effectively only unlink the creator " +
      "if creator email and name cannot be found in GitLab" in new TestCase {

      val forkInGitLab = projectPaths.generateOne
      val gitLabProject = given {
        gitLabProjects(forkInGitLab).generateOne.copy(
          maybeCreator = gitLabCreator(maybeEmail = None).generateSome.map(_.copy(maybeName = None))
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne
      }.existsInKG

      val wasDerivedFromInsert = (updatesCreator
        .insertWasDerivedFrom[IO](_: ResourceId, _: Path)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, forkInGitLab, *)
        .returningUpdates

      val creatorUpdates = (updatesCreator
        .addNewCreator[IO](_: ResourceId, _: Option[Email], _: Option[users.Name])(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId,
                 gitLabProject.maybeCreator.flatMap(_.maybeEmail),
                 gitLabProject.maybeCreator.flatMap(_.maybeName),
                 *)
        .returningUpdates

      val recreateDateCreated = (updatesCreator
        .recreateDateCreated[IO](_: ResourceId, _: DateCreated)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, gitLabProject.dateCreated, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List(
          wasDerivedFromInsert,
          creatorUpdates,
          recreateDateCreated
        ).flatten
      )
    }

    "fail if finding Person with an email in the KG fails" in new TestCase {

      val emailInGitLab = userEmails.generateOne
      given {
        gitLabProjects(projectPaths.generateOne).generateOne.copy(
          maybeCreator = gitLabCreator(Some(emailInGitLab)).generateSome
        )
      }.existsInGitLab

      given {
        kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne
      }.existsInKG

      val exception = exceptions.generateOne
      (kgInfoFinder
        .findCreatorId(_: Email))
        .expects(emailInGitLab)
        .returning(exception.raiseError[IO, Option[users.ResourceId]])

      intercept[Exception] {
        updater.create(event).value.unsafeRunSync()
      } shouldBe exception
    }
  }

  "updateForkInfo - cases when fork only in KG" should {

    "remove the wasDerivedFrom triple, recreate dateCreated and link a new creator " +
      "if there's a Person with the new email already in the KG" in new TestCase {

      val forkInGitLab  = projectPaths.generateOne
      val emailInGitLab = userEmails.generateOne
      val gitLabProject = given {
        gitLabProjects(maybeParentPaths = emptyOptionOf[Path]).generateOne.copy(
          maybeCreator = gitLabCreator(Some(emailInGitLab)).generateSome
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(projectResourceIds.toGeneratorOfSomes).generateOne
      }.existsInKG

      val wasDerivedFromDelete = (updatesCreator
        .deleteWasDerivedFrom[IO](_: ResourceId)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, *)
        .returningUpdates

      val newCreatorId = userResourceIds(Some(emailInGitLab)).generateOne
      given(
        newCreatorId = Some(newCreatorId),
        forEmail     = emailInGitLab
      ).existsInKG
      val creatorUpdates = (updatesCreator
        .swapCreator[IO](_: ResourceId, _: users.ResourceId)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, newCreatorId, *)
        .returningUpdates

      val recreateDateCreated = (updatesCreator
        .recreateDateCreated[IO](_: ResourceId, _: DateCreated)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, gitLabProject.dateCreated, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List(
          wasDerivedFromDelete,
          creatorUpdates,
          recreateDateCreated
        ).flatten
      )
    }

    "remove the wasDerivedFrom triple, recreate dateCreated and creator " +
      "and link the new creator to the project in KG " +
      "if there is no Person with the new email in the KG yet" in new TestCase {

      val forkInGitLab  = projectPaths.generateOne
      val emailInGitLab = userEmails.generateOne
      val gitLabProject = given {
        gitLabProjects(maybeParentPaths = emptyOptionOf[Path]).generateOne.copy(
          maybeCreator = gitLabCreator(Some(emailInGitLab)).generateSome
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(projectResourceIds.toGeneratorOfSomes).generateOne
      }.existsInKG

      val wasDerivedFromDelete = (updatesCreator
        .deleteWasDerivedFrom[IO](_: ResourceId)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, *)
        .returningUpdates

      given(newCreatorId = None, forEmail = emailInGitLab).existsInKG
      val creatorUpdates = (updatesCreator
        .addNewCreator[IO](_: ResourceId, _: Option[Email], _: Option[users.Name])(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId,
                 gitLabProject.maybeCreator.flatMap(_.maybeEmail),
                 gitLabProject.maybeCreator.flatMap(_.maybeName),
                 *)
        .returningUpdates

      val recreateDateCreated = (updatesCreator
        .recreateDateCreated[IO](_: ResourceId, _: DateCreated)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, gitLabProject.dateCreated, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List(
          wasDerivedFromDelete,
          creatorUpdates,
          recreateDateCreated
        ).flatten
      )
    }

    "remove the wasDerivedFrom triple, recreate dateCreated and create and link a new creator " +
      "if there's only creator username in GitLab" in new TestCase {

      val forkInGitLab = projectPaths.generateOne
      val gitLabProject = given {
        gitLabProjects(maybeParentPaths = emptyOptionOf[Path]).generateOne.copy(
          maybeCreator = gitLabCreator(maybeEmail = None).generateSome.map(_.copy(maybeName = userNames.generateSome))
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(projectResourceIds.toGeneratorOfSomes).generateOne
      }.existsInKG

      val wasDerivedFromDelete = (updatesCreator
        .deleteWasDerivedFrom[IO](_: ResourceId)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, *)
        .returningUpdates

      val creatorUpdates = (updatesCreator
        .addNewCreator[IO](_: ResourceId, _: Option[Email], _: Option[users.Name])(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId,
                 gitLabProject.maybeCreator.flatMap(_.maybeEmail),
                 gitLabProject.maybeCreator.flatMap(_.maybeName),
                 *)
        .returningUpdates

      val recreateDateCreated = (updatesCreator
        .recreateDateCreated[IO](_: ResourceId, _: DateCreated)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, gitLabProject.dateCreated, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List(
          wasDerivedFromDelete,
          creatorUpdates,
          recreateDateCreated
        ).flatten
      )
    }

    "remove the wasDerivedFrom triple, recreate dateCreated and effectively only unlink the creator " +
      "if creator email and name cannot be found in GitLab" in new TestCase {

      val forkInGitLab = projectPaths.generateOne
      val gitLabProject = given {
        gitLabProjects(maybeParentPaths = emptyOptionOf[Path]).generateOne.copy(
          maybeCreator = gitLabCreator(maybeEmail = None).generateSome.map(_.copy(maybeName = None))
        )
      }.existsInGitLab

      val kgProject = given {
        kgProjects(projectResourceIds.toGeneratorOfSomes).generateOne
      }.existsInKG

      val wasDerivedFromDelete = (updatesCreator
        .deleteWasDerivedFrom[IO](_: ResourceId)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, *)
        .returningUpdates

      val creatorUpdates = (updatesCreator
        .addNewCreator[IO](_: ResourceId, _: Option[Email], _: Option[users.Name])(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId,
                 gitLabProject.maybeCreator.flatMap(_.maybeEmail),
                 gitLabProject.maybeCreator.flatMap(_.maybeName),
                 *)
        .returningUpdates

      val recreateDateCreated = (updatesCreator
        .recreateDateCreated[IO](_: ResourceId, _: DateCreated)(_: MonadError[IO, Throwable]))
        .expects(kgProject.resourceId, gitLabProject.dateCreated, *)
        .returningUpdates

      updater.create(event).value.unsafeRunSync() shouldBe Right(
        List(
          wasDerivedFromDelete,
          creatorUpdates,
          recreateDateCreated
        ).flatten
      )
    }
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val event = commitEvents.generateOne

    val gitLabInfoFinder = mock[GitLabInfoFinder[IO]]
    val kgInfoFinder     = mock[KGInfoFinder[IO]]
    val updatesCreator   = mock[UpdatesCreator]
    val updater          = new UpdateFunctionsCreatorImpl(gitLabInfoFinder, kgInfoFinder, updatesCreator)

    def given(gitLabProject: GitLabProject) = new {
      lazy val existsInGitLab: GitLabProject = {
        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(Option(gitLabProject).pure[IO])
        gitLabProject
      }

      lazy val doesNotExistsInGitLab = {
        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(Option.empty.pure[IO])
      }
    }

    def given(kgProject: KGProject) = new {
      lazy val existsInKG: KGProject = {
        (kgInfoFinder
          .findProject(_: Path))
          .expects(event.project.path)
          .returning(Option(kgProject).pure[IO])
        kgProject
      }

      lazy val doesNotExistsInKG = {
        (kgInfoFinder
          .findProject(_: Path))
          .expects(event.project.path)
          .returning(Option.empty.pure[IO])
      }
    }

    def given(newCreatorId: Option[users.ResourceId], forEmail: Email) = new {
      lazy val existsInKG = {
        (kgInfoFinder
          .findCreatorId(_: Email))
          .expects(forEmail)
          .returning(newCreatorId.pure[IO])
      }
    }

    implicit class CallHandlerOps(handler: CallHandler[List[CuratedTriples.UpdateFunction[IO]]]) {
      private val updates = listOf(curationUpdateFunctions[IO], maxElements = 3).generateOne

      lazy val returningUpdates: List[CuratedTriples.UpdateFunction[IO]] = {
        handler.returning(updates)
        updates
      }
    }
  }
}
