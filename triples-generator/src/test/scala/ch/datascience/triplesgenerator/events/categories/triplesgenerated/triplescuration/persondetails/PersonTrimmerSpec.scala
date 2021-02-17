package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

private class PersonTrimmerSpec extends AnyWordSpec with should.Matchers with MockFactory {
  "getTriplesAndTrimmedPersons" should {
    "???" in new TestCase {}
  }

  trait TestCase {
    val personExtractor      = mock[PersonExtractor]
    val commitCommiterFinder = mock[CommitCommitterFinder[Try]]
    val personTrimmer        = new PersonTrimmerImpl[Try](personExtractor, commitCommiterFinder)
  }
}
