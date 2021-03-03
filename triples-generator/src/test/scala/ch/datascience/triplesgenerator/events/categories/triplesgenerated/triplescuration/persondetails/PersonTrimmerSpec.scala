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

import cats.data.{EitherT, NonEmptyList}
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{exceptions, listOf}
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.{userEmails, userNames, userResourceIds, _}
import ch.datascience.graph.model.events.EventId
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails.PersonDetailsGenerators.commitPersons
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

private class PersonTrimmerSpec extends AnyWordSpec with should.Matchers with MockFactory {
  "getTriplesAndTrimmedPersons" should {

    "return person with one name " +
      "case when the person has one name and one email" in new TestCase {

        val email        = userEmails.generateOne
        val expectedName = userNames.generateOne
        val personData   = PersonRawData(userResourceIds.generateOne, List(expectedName), List(email))

        (personExtractor.extractPersons _)
          .expects(triples)
          .returning((expectedTriples, Set(personData)))

        val expectedPerson = Person(personData.id, None, expectedName, email.some)

        personTrimmer.getTriplesAndTrimmedPersons(triples, projectId, eventId, maybeAccessToken) shouldBe Success(
          expectedTriples -> Set(expectedPerson)
        )
      }

    "return person with one name " +
      "case when the person has multiple names and one email" in new TestCase {

        val email        = userEmails.generateOne
        val expectedName = userNames.generateOne
        val personData =
          PersonRawData(userResourceIds.generateOne,
                        userNames.toGeneratorOfNonEmptyList(2).generateOne.toList :+ expectedName,
                        List(email)
          )

        val commitPerson       = CommitPerson(expectedName, email)
        val otherCommitPersons = listOf(commitPersons).generateOne
        (personExtractor.extractPersons _)
          .expects(triples)
          .returning((expectedTriples, Set(personData)))

        val expectedPerson = Person(personData.id, None, expectedName, email.some)

        (commitCommiterFinder.findCommitPeople _)
          .expects(projectId, commitId, maybeAccessToken)
          .returning(
            EitherT.right(Success(CommitPersonsInfo(commitId, NonEmptyList(commitPerson, otherCommitPersons))))
          )

        personTrimmer.getTriplesAndTrimmedPersons(triples, projectId, eventId, maybeAccessToken) shouldBe Success(
          expectedTriples -> Set(
            expectedPerson
          )
        )
      }

    "return person with one name " +
      "case when the person has no name and one email" in new TestCase {

        val email        = userEmails.generateOne
        val expectedName = userNames.generateOne
        val personData   = PersonRawData(userResourceIds.generateOne, List.empty[Name], List(email))

        val commitPerson       = CommitPerson(expectedName, email)
        val otherCommitPersons = listOf(commitPersons).generateOne
        (personExtractor.extractPersons _)
          .expects(triples)
          .returning((expectedTriples, Set(personData)))

        val expectedPerson = Person(personData.id, None, expectedName, email.some)

        (commitCommiterFinder.findCommitPeople _)
          .expects(projectId, commitId, maybeAccessToken)
          .returning(
            EitherT.right(Success(CommitPersonsInfo(commitId, NonEmptyList(commitPerson, otherCommitPersons))))
          )

        personTrimmer.getTriplesAndTrimmedPersons(triples, projectId, eventId, maybeAccessToken) shouldBe Success(
          expectedTriples -> Set(
            expectedPerson
          )
        )
      }

    "return person with one name " +
      "case when the person has one name and no email" in new TestCase {
        val expectedName = userNames.generateOne
        val personData   = PersonRawData(userResourceIds.generateOne, List(expectedName), List.empty[Email])

        (personExtractor.extractPersons _)
          .expects(triples)
          .returning((expectedTriples, Set(personData)))

        val expectedPerson = Person(personData.id, None, expectedName, None)

        personTrimmer.getTriplesAndTrimmedPersons(triples, projectId, eventId, maybeAccessToken) shouldBe Success(
          expectedTriples -> Set(
            expectedPerson
          )
        )
      }

    "fail if there's a Person entity with multiple emails" in new TestCase {

      val emails = userEmails.toGeneratorOfNonEmptyList(2).generateOne.toList
      val names  = listOf(userNames).generateOne
      val entityId: ResourceId = userResourceIds.generateOne
      val personData = PersonRawData(entityId, names, emails)

      (personExtractor.extractPersons _)
        .expects(triples)
        .returning((expectedTriples, Set(personData)))

      val Failure(exception) =
        personTrimmer.getTriplesAndTrimmedPersons(triples, projectId, eventId, maybeAccessToken).value

      exception.getMessage shouldBe s"Multiple emails for person with '$entityId' id found in generated JSON-LD"
    }

    "fail if there's a Person entity with multiple names and no email" in new TestCase {

      val expectedNames = userNames.toGeneratorOfNonEmptyList(2).generateOne.toList
      val personData    = PersonRawData(userResourceIds.generateOne, expectedNames, List.empty[Email])

      (personExtractor.extractPersons _)
        .expects(triples)
        .returning((expectedTriples, Set(personData)))

      val Failure(exception) =
        personTrimmer.getTriplesAndTrimmedPersons(triples, projectId, eventId, maybeAccessToken).value
      exception.getMessage shouldBe s"No email for person with id '${personData.id}' and multiple names found in generated JSON-LD"
    }

    "fail if there's a Person entity with no name and no email" in new TestCase {

      val id: ResourceId = userResourceIds.generateOne
      val personData = PersonRawData(id, List.empty[Name], List.empty[Email])

      (personExtractor.extractPersons _)
        .expects(triples)
        .returning((expectedTriples, Set(personData)))

      val Failure(exception) =
        personTrimmer.getTriplesAndTrimmedPersons(triples, projectId, eventId, maybeAccessToken).value
      exception.getMessage shouldBe s"No email and no name for person with id '$id' found in generated JSON-LD"
    }

    "fail if there's a Person entity with an email and multiple names but the person is not in the gitlab result" in new TestCase {

      val names = userNames.toGeneratorOfNonEmptyList(2).generateOne.toList
      val email = userEmails.generateOne
      val id: ResourceId = userResourceIds.generateOne
      val personData = PersonRawData(id, names, List(email))

      val otherCommitPersons = commitPersons.toGeneratorOfNonEmptyList(2).generateOne

      (personExtractor.extractPersons _)
        .expects(triples)
        .returning((expectedTriples, Set(personData)))

      (commitCommiterFinder.findCommitPeople _)
        .expects(projectId, commitId, maybeAccessToken)
        .returning(EitherT.right(Success(CommitPersonsInfo(commitId, otherCommitPersons))))

      val Failure(exception: Exception) =
        personTrimmer.getTriplesAndTrimmedPersons(triples, projectId, eventId, maybeAccessToken).value
      exception.getMessage shouldBe s"Could not find the email for person with id '$id' in gitlab"
    }

    "fail if there's a Person entity with an email but no name and the person is not in the gitlab result" in new TestCase {

      val email = userEmails.generateOne
      val id: ResourceId = userResourceIds.generateOne
      val personData = PersonRawData(id, List.empty[Name], List(email))

      val otherCommitPersons = commitPersons.toGeneratorOfNonEmptyList(2).generateOne

      (personExtractor.extractPersons _)
        .expects(triples)
        .returning((expectedTriples, Set(personData)))

      (commitCommiterFinder.findCommitPeople _)
        .expects(projectId, commitId, maybeAccessToken)
        .returning(EitherT.right(Success(CommitPersonsInfo(commitId, otherCommitPersons))))

      val Failure(exception: Exception) =
        personTrimmer.getTriplesAndTrimmedPersons(triples, projectId, eventId, maybeAccessToken).value
      exception.getMessage shouldBe s"Could not find the email for person with id '$id' in gitlab"
    }

    "fail if the commit finder fails" in new TestCase {

      val personsRawData = personRawDataWithOneEmail.toGeneratorOfNonEmptyList().generateOne.toList.toSet

      val expectedException = exceptions.generateOne

      (personExtractor.extractPersons _)
        .expects(triples)
        .returning((expectedTriples, personsRawData))

      (commitCommiterFinder.findCommitPeople _)
        .expects(projectId, commitId, maybeAccessToken)
        .returning(EitherT.right(expectedException.raiseError[Try, CommitPersonsInfo]))

      val Failure(exception) =
        personTrimmer.getTriplesAndTrimmedPersons(triples, projectId, eventId, maybeAccessToken).value

      exception.getMessage shouldBe expectedException.getMessage
    }

  }

  trait TestCase {
    val personExtractor      = mock[PersonExtractor]
    val commitCommiterFinder = mock[CommitCommitterFinder[Try]]
    val personTrimmer        = new PersonTrimmerImpl[Try](personExtractor, commitCommiterFinder)

    val maybeAccessToken = accessTokens.generateOption
    val triples          = jsonLDTriples.generateOne
    val expectedTriples  = jsonLDTriples.generateOne
    val projectId        = projectIds.generateOne
    val commitId         = commitIds.generateOne
    val eventId          = EventId(commitId.value)

  }

  lazy val personRawDataWithMultipleNames: Gen[PersonRawData] = for {
    id     <- userResourceIds
    names  <- userNames.toGeneratorOfNonEmptyList(2)
    emails <- listOf(userEmails)
  } yield PersonRawData(id, names.toList, emails)

  lazy val personRawDataWithOneEmail: Gen[PersonRawData] = for {
    id     <- userResourceIds
    names  <- userNames.toGeneratorOfNonEmptyList(2)
    emails <- userEmails
  } yield PersonRawData(id, names.toList, List(emails))

}
