package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{exceptions, listOf}
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.{userEmails, userNames, userResourceIds, _}
import ch.datascience.graph.model.events.EventId
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CurationGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import eu.timepit.refined.auto._
import org.scalacheck.Gen

import scala.util.{Failure, Success, Try}

private class PersonTrimmerSpec extends AnyWordSpec with should.Matchers with MockFactory {
  "getTriplesAndTrimmedPersons" should {

    "return person with one name" +
      "case when the person has one email and multiple names" in new TestCase {

        val email        = userEmails.generateOne
        val expectedName = userNames.generateOne
        val personData =
          (userResourceIds.generateOne,
           userNames.toGeneratorOfNonEmptyList(2).generateOne.toList :+ expectedName,
           List(email)
          )

        val commitPerson       = CommitPerson(expectedName, email)
        val otherCommitPersons = listOf(commitPersons).generateOne
        (personExtractor.extractPersons _)
          .expects(triples)
          .returning((expectedTriples, Set(personData)))

        val expectedPerson = Person(personData._1, None, expectedName, email.some)

        (commitCommiterFinder.findCommitPeople _)
          .expects(projectPath, commitId)
          .returning(Success(otherCommitPersons.toSet + commitPerson))

        personTrimmer.getTriplesAndTrimmedPersons(triples, eventId) shouldBe (expectedTriples, expectedPerson)
      }

    "return person with one name" +
      "case when the person has one email and no name" in new TestCase {

        val email        = userEmails.generateOne
        val expectedName = userNames.generateOne
        val personData   = (userResourceIds.generateOne, List.empty[Name], List(email))

        val commitPerson       = CommitPerson(expectedName, email)
        val otherCommitPersons = listOf(commitPersons).generateOne
        (personExtractor.extractPersons _)
          .expects(triples)
          .returning((expectedTriples, Set(personData)))

        val expectedPerson = Person(personData._1, None, expectedName, email.some)

        (commitCommiterFinder.findCommitPeople _)
          .expects(projectPath, commitId)
          .returning(Success(otherCommitPersons.toSet + commitPerson))

        personTrimmer.getTriplesAndTrimmedPersons(triples, eventId) shouldBe (expectedTriples, expectedPerson)
      }

    List(
      ("multiple names",
       userNames.toGeneratorOfNonEmptyList(2).generateOne.toList,
       "Multiple names for person in generated JSON-LD"
      ),
      ("no name", List.empty[Name], "No names for person in generated JSON-LD")
    )
      .foreach { case (title, names, exceptionMessage) =>
        s"fail if there's a Person entity with $title and gitlab does not find the person" in new TestCase {

          val email      = userEmails.generateOne
          val personData = (userResourceIds.generateOne, names, List(email))

          val otherCommitPersons = listOf(commitPersons).generateOne

          (personExtractor.extractPersons _)
            .expects(triples)
            .returning((expectedTriples, Set(personData)))

          (commitCommiterFinder.findCommitPeople _)
            .expects(projectPath, commitId)
            .returning(Success(otherCommitPersons.toSet))

          val Failure(exception: Exception) = personTrimmer.getTriplesAndTrimmedPersons(triples, eventId)
          exception.getMessage shouldBe exceptionMessage
        }
      }

    "return person with one name" +
      "case when the person has one name and one email" in new TestCase {

        val email        = userEmails.generateOne
        val expectedName = userNames.generateOne
        val personData   = (userResourceIds.generateOne, List(expectedName), List(email))

        (personExtractor.extractPersons _)
          .expects(triples)
          .returning((expectedTriples, Set(personData)))

        val expectedPerson = Person(personData._1, None, expectedName, email.some)

        personTrimmer.getTriplesAndTrimmedPersons(triples, eventId) shouldBe (expectedTriples, expectedPerson)
      }

    "fail if there's a Person entity with multiple emails" in new TestCase {

      val emails = userEmails.toGeneratorOfNonEmptyList(2).generateOne.toList
      val names  = listOf(userNames).generateOne
      val entityId: ResourceId = userResourceIds.generateOne
      val personData = (entityId, names, emails)

      (personExtractor.extractPersons _)
        .expects(triples)
        .returning((expectedTriples, Set(personData)))

      val Failure(exception) = personTrimmer.getTriplesAndTrimmedPersons(triples, eventId)

      exception.getMessage shouldBe s"Multiple emails for person with '$entityId' id found in generated JSON-LD"
    }

    "fail if the commit finder fails" in new TestCase {

      val personsRawData = personRawDataWithMultipleNames.toGeneratorOfNonEmptyList().generateOne.toList.toSet

      val expectedException = exceptions.generateOne

      (personExtractor.extractPersons _)
        .expects(triples)
        .returning((expectedTriples, personsRawData))

      (commitCommiterFinder.findCommitPeople _)
        .expects(projectPath, commitId)
        .returning(expectedException.raiseError[Try, Set[CommitPerson]])

      val Failure(exception) = personTrimmer.getTriplesAndTrimmedPersons(triples, eventId)

      exception.getMessage shouldBe expectedException.getMessage
    }

  }

  trait TestCase {
    val personExtractor      = mock[PersonExtractor]
    val commitCommiterFinder = mock[CommitCommitterFinder[Try]]
    val personTrimmer        = new PersonTrimmerImpl[Try](personExtractor, commitCommiterFinder)

    val triples         = jsonLDTriples.generateOne
    val expectedTriples = jsonLDTriples.generateOne
    val projectPath     = projectPaths.generateOne
    val commitId        = commitIds.generateOne
    val eventId         = EventId(commitId.value)

  }

  type PersonRawDatum = (ResourceId, List[Name], List[Email])

  lazy val personRawDataWithMultipleNames: Gen[PersonRawDatum] = for {
    id     <- userResourceIds
    names  <- userNames.toGeneratorOfNonEmptyList(2)
    emails <- listOf(userEmails)
  } yield (id, names.toList, emails)

}
