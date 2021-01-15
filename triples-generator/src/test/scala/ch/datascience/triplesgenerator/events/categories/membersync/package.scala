package ch.datascience.triplesgenerator.events.categories

import cats.syntax.all._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.users.ResourceId
import ch.datascience.rdfstore.entities.Person
import io.renku.jsonld.syntax._

package object membersync {

  private[membersync] implicit class PersonsOps(persons: Set[Person])(implicit
      renkuBaseUrl:                                      RenkuBaseUrl,
      gitLabApiUrl:                                      GitLabApiUrl
  ) {

    lazy val toKGProjectMembers: Set[KGProjectMember] = persons.flatMap { member =>
      (member.asJsonLD.entityId.map(ResourceId.apply) -> member.maybeGitLabId)
        .mapN { case (resourceId, gitLabId) =>
          KGProjectMember(resourceId, gitLabId)
        }
    }
  }
}
