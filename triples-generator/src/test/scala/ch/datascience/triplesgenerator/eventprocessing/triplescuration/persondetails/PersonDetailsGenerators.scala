package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.PersonDetailsUpdater.Person
import org.scalacheck.Gen

private object PersonDetailsGenerators {
  implicit val gitLabProjectMembers: Gen[GitLabProjectMember] = for {
    id       <- userGitLabIds
    username <- usernames
    name     <- userNames
  } yield GitLabProjectMember(id, username, name)

  implicit val persons: Gen[Person] = for {
    id         <- userResourceIds
    name       <- userNames
    maybeEmail <- userEmails.toGeneratorOfOptions
  } yield Person(id, maybeGitLabId = None, name, maybeEmail)
}
