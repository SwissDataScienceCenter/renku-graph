package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.{userEmails, userNames, userResourceIds}
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import io.renku.jsonld.generators.Generators.Implicits.asArbitrary
import org.scalacheck.Prop.forAll
import eu.timepit.refined.auto._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._

private class PersonTrimmerSpec
    extends AnyWordSpec
    with should.Matchers
    with MockFactory
    with ScalaCheckPropertyChecks {
  "getTriplesAndTrimmedPersons" should {
    "return triples without person raw data and deduplicate names" in new TestCase {

      forAll { (personRawData: Set[PersonRawDatum], commitPersons: Set[CommitPerson]) =>
        val expectedTriples = jsonLDTriples.generateOne
        val personRawDatumWithMultipleNames: PersonRawDatum =
          (userResourceIds.generateOne, userNames.generateList(3), listOf(userEmails).generateOne)

        val projectPath = projectPaths.generateOne
        val commitId    = commitIds.generateOne

        (personExtractor.extractPersons _)
          .expects(triples)
          .returning((expectedTriples, personRawData))

        (commitCommiterFinder.findCommitPeople _)
          .expects(projectPath, commitId)
          .returning(Success(commitPersons))

        val expectedPersons = ???

        personTrimmer.getTriplesAndTrimmedPersons(triples, eventId) shouldBe (expectedTriples, expectedPersons)
      }

    }
  }

  trait TestCase {
    val personExtractor      = mock[PersonExtractor]
    val commitCommiterFinder = mock[CommitCommitterFinder[Try]]
    val personTrimmer        = new PersonTrimmerImpl[Try](personExtractor, commitCommiterFinder)

    val triples = jsonLDTriples.generateOne
    val eventId = eventIds.generateOne
  }

  type PersonRawDatum = (ResourceId, List[Name], List[Email])

}
