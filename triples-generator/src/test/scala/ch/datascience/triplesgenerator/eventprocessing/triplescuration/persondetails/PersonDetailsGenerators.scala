package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails
import ch.datascience.graph.model.GraphModelGenerators._
import org.scalacheck.Gen

private object PersonDetailsGenerators {
  implicit val gitLabProjectMembers: Gen[GitLabProjectMember] = for {
    id       <- userGitLabIds
    username <- usernames
    name     <- userNames
  } yield GitLabProjectMember(id, username, name)

}
