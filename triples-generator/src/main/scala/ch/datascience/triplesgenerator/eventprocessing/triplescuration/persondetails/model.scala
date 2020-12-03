package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import ch.datascience.graph.model.users.{GitLabId, Name, Username}

final case class GitLabProjectMember(id: GitLabId, username: Username, name: Name)
