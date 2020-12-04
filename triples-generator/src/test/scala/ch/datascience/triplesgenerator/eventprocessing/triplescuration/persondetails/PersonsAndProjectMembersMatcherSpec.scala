package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import PersonDetailsGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.booleans
import ch.datascience.graph.model.users.Username
import eu.timepit.refined.auto._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PersonsAndProjectMembersMatcherSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "merge" should {

    "supply each person with a GitLab Id when it is matched in the members list" in new TestCase {
      forAll(persons.toGeneratorOfNonEmptyList(maxElements = 20).map(_.toList.toSet)) { personsWithoutId =>
        val personsAndMembers = personsWithoutId map { person =>
          val member =
            if (booleans.generateOne) gitLabProjectMembers.generateOne.copy(name = person.name)
            else gitLabProjectMembers.generateOne.copy(username = Username(person.name.value))
          val personWithId = person.copy(maybeGitLabId = Some(member.id))
          personWithId -> member
        }

        matcher.merge(personsWithoutId, personsAndMembers.map(_._2)) shouldBe personsAndMembers.map(_._1)
      }
    }

    "leave person without GitLab Id when it there's no matching member" in new TestCase {

      val personsWithoutId = persons.generateNonEmptyList(minElements = 2).toList.toSet
      val personsAndMembers = personsWithoutId.tail map { person =>
        val member =
          if (booleans.generateOne) gitLabProjectMembers.generateOne.copy(name = person.name)
          else gitLabProjectMembers.generateOne.copy(username = Username(person.name.value))
        val personWithId = person.copy(maybeGitLabId = Some(member.id))
        personWithId -> member
      }

      matcher.merge(personsWithoutId, personsAndMembers.map(_._2)) shouldBe
        (personsAndMembers.map(_._1) + personsWithoutId.head)
    }

    "do nothing if no persons given" in new TestCase {
      matcher.merge(Set.empty, gitLabProjectMembers.generateNonEmptyList().toList.toSet) shouldBe Set.empty
    }

    "do nothing if no members given" in new TestCase {

      val personsWithoutId = persons.generateNonEmptyList(minElements = 2).toList.toSet

      matcher.merge(personsWithoutId, Set.empty) shouldBe personsWithoutId
    }
  }

  private trait TestCase {
    val matcher = new PersonsAndProjectMembersMatcher()
  }
}
