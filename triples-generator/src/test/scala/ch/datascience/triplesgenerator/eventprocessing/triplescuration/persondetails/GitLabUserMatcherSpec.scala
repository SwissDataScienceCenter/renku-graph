package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.booleans
import ch.datascience.graph.model.users.Username
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.PersonDetailsGenerators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
class GitLabUserMatcherSpec extends AnyWordSpec with should.Matchers {

  "matchPersonWithGitLabProjectMember" should {
    "supply each person with a GitLab Id when it is matched in the members list" in new TestCase {

      val people = persons.generateNonEmptyList().toList.toSet
      val members = people.map { person =>
        val genMember = gitLabProjectMembers.generateOne
        if (booleans.generateOne) genMember.copy(name = person.name)
        else genMember.copy(username = Username(person.name.value))
      }

      matcher.matchPersonWithGitLabProjectMember(people, members)
    }
  }

  private trait TestCase {
    val matcher = new GitLabUserMatcher()
  }
}
