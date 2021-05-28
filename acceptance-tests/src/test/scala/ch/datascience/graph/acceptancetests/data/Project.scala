package ch.datascience.graph.acceptancetests.data

import ch.datascience.graph.model.projects.{Description, Id, Name, Path}
import ch.datascience.knowledgegraph.projects.model.Project.{DateUpdated, StarsCount, Tag}
import ch.datascience.knowledgegraph.projects.model.{Permissions, Statistics, Urls}
import ch.datascience.rdfstore.entities

final case class Project[FC <: entities.Project.ForksCount](entitiesProject:  entities.Project[FC],
                                                            id:               Id,
                                                            maybeDescription: Option[Description],
                                                            updatedAt:        DateUpdated,
                                                            urls:             Urls,
                                                            tags:             Set[Tag],
                                                            starsCount:       StarsCount,
                                                            permissions:      Permissions,
                                                            statistics:       Statistics
) {
  val path: Path = entitiesProject.path
  val name: Name = entitiesProject.name
}
