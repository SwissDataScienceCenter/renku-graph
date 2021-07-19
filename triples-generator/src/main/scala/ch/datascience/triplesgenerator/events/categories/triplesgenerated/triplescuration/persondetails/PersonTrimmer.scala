/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrlLoader
import ch.datascience.graph.model.entities.Person
import ch.datascience.graph.model.events.EventId
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.graph.model.{events, projects}
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.ProjectMetadata
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait PersonTrimmer[Interpretation[_]] {
  def getTriplesAndTrimmedPersons(projectMetadata: ProjectMetadata, eventId: EventId)(implicit
      maybeAccessToken:                            Option[AccessToken]
  ): EitherT[Interpretation, ProcessingRecoverableError, (JsonLDTriples, Set[Person])]
}

private class PersonTrimmerImpl[Interpretation[_]: MonadThrow](
    commitCommitterFinder: CommitCommitterFinder[Interpretation]
) extends PersonTrimmer[Interpretation] {

  override def getTriplesAndTrimmedPersons(projectMetadata: ProjectMetadata, eventId: EventId)(implicit
      maybeAccessToken:                                     Option[AccessToken]
  ): EitherT[Interpretation, ProcessingRecoverableError, (JsonLDTriples, Set[Person])] = ???
//    for {
//    _       <- findAllPersons(projectMetadata)
//    persons <- trimPersons(personsWithMaybeEmail, projectId, CommitId(eventId.value), maybeAccessToken)
//  } yield (JsonLDTriples(List.empty), Set.empty)

  private def findAllPersons(
      projectMetadata: ProjectMetadata
  ): EitherT[Interpretation, ProcessingRecoverableError, Set[Person]] = ???

  private def trimPersons(personsRawData: Set[RawDataMaybeEmail], projectId: projects.Id, commitId: events.CommitId)(
      implicit maybeAccessToken:          Option[AccessToken]
  ): EitherT[Interpretation, ProcessingRecoverableError, Set[Person]] = {
    val (personsDataWithNoEmail, personsDataWithSingleEmail)     = partitionPersonsWithSingleEmail(personsRawData)
    val (personsFromSingleEmail, withSingleEmailToCheckInGitlab) = personsDataWithSingleEmail.toPersonOrToGitlab
    val personsWithNoEmail                                       = personsDataWithNoEmail.toPersonsOrThrow.toRightT
    if (withSingleEmailToCheckInGitlab.nonEmpty) {
      for {
        commitPersonInfo <- commitCommitterFinder.findCommitPeople(projectId, commitId, maybeAccessToken)
        personsWithDeduplicatedEmails <-
          mergeRawDataWithGitLabData(withSingleEmailToCheckInGitlab, commitPersonInfo).toRightT

        personsWithoutEmails <- personsWithNoEmail
      } yield personsWithDeduplicatedEmails ++ personsWithoutEmails ++ personsFromSingleEmail
    } else {
      personsWithNoEmail.map(_ ++ personsFromSingleEmail)
    }
  }

  private def mergeRawDataWithGitLabData(personsRawData:   Set[RawDataDisregardNameSingleEmail],
                                         commitPersonInfo: CommitPersonsInfo
  ): Interpretation[Set[Person]] =
    personsRawData
      .map { disregardNameSingleEmail =>
        commitPersonInfo.committers
          .find(_.email == disregardNameSingleEmail.email)
          .map(committer =>
            Person(disregardNameSingleEmail.id, committer.name, Nil, committer.email.some, None, None)
              .pure[Interpretation]
          )
          .getOrElse(
            new Exception(s"Could not find the email for person with id '${disregardNameSingleEmail.id}' in gitlab")
              .raiseError[Interpretation, Person]
          )
      }
      .toList
      .sequence
      .map(_.toSet)

  private def partitionPersonsWithSingleEmail(
      personsRawData: Set[RawDataMaybeEmail]
  ): (Set[RawDataNoEmail], Set[RawDataSingleEmail]) =
    personsRawData.foldLeft((Set.empty[RawDataNoEmail], Set.empty[RawDataSingleEmail])) {
      case ((rawDataNoEmail, rawDataWithSingleEmail), RawDataMaybeEmail(id, names, Some(email))) =>
        rawDataNoEmail -> (rawDataWithSingleEmail + RawDataSingleEmail(id, names, email))
      case ((rawDataNoEmail, rawDataWithSingleEmail), RawDataMaybeEmail(id, names, None)) =>
        (rawDataNoEmail + RawDataNoEmail(id, names)) -> rawDataWithSingleEmail
    }

//  private implicit class PersonsRawDataOps(persons: Set[PersonRawData]) {
//    lazy val toMaybeEmail: Set[RawDataMaybeEmail] =
//      persons.map(rawPerson => RawDataMaybeEmail(rawPerson.id, rawPerson.names, rawPerson.emails.headOption))
//  }

  private implicit class PersonsRawDataSingleEmailOps(persons: Set[RawDataSingleEmail]) {

    lazy val toPersonOrToGitlab: (Set[Person], Set[RawDataDisregardNameSingleEmail]) =
      persons.foldLeft((Set.empty[Person], Set.empty[RawDataDisregardNameSingleEmail])) {
        case ((persons, personsWithMultipleNames), RawDataSingleEmail(id, name :: Nil, email)) =>
          (persons + Person(id, name, Nil, email.some, None, None)) -> personsWithMultipleNames
        case ((persons, personsWithMultipleNames), RawDataSingleEmail(id, _, email)) =>
          persons -> (personsWithMultipleNames + RawDataDisregardNameSingleEmail(id, email))
      }
  }

  private implicit class PersonsRawDataWithNoEmailOps(persons: Set[RawDataNoEmail]) {
    lazy val toPersonsOrThrow: Interpretation[Set[Person]] = persons
      .map {
        case RawDataNoEmail(id, Nil) =>
          new Exception(s"No email and no name for person with id '$id' found in generated JSON-LD")
            .raiseError[Interpretation, Person]
        case RawDataNoEmail(id, name :: Nil) => Person(id, name, Nil, None, None, None).pure[Interpretation]
        case RawDataNoEmail(id, _) =>
          new Exception(s"No email for person with id '$id' and multiple names found in generated JSON-LD")
            .raiseError[Interpretation, Person]
      }
      .toList
      .sequence
      .map(_.toSet)
  }

  private implicit class ResultOps[T](out: Interpretation[T]) {
    lazy val toRightT: EitherT[Interpretation, ProcessingRecoverableError, T] =
      EitherT.right[ProcessingRecoverableError](out)
  }

}

private final case class RawDataMaybeEmail(id: ResourceId, names: List[Name], maybeEmail: Option[Email])
private final case class RawDataDisregardNameSingleEmail(id: ResourceId, email: Email)
private final case class RawDataSingleEmail(id: ResourceId, names: List[Name], email: Email)
private final case class RawDataNoEmail(id: ResourceId, names: List[Name])

private object IOPersonTrimmer {
  def apply(gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO])(implicit
      executionContext:      ExecutionContext,
      contextShift:          ContextShift[IO],
      timer:                 Timer[IO]
  ): IO[PersonTrimmer[IO]] = for {
    gitLabApiUrl          <- GitLabUrlLoader[IO]().map(_.apiV4)
    commitCommitterFinder <- IOCommitCommitterFinder(gitLabApiUrl, gitLabThrottler, logger)
  } yield new PersonTrimmerImpl[IO](commitCommitterFinder)
}
