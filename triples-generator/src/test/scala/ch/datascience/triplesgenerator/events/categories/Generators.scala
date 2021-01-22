package ch.datascience.triplesgenerator.events.categories

import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import ch.datascience.triplesgenerator.events.categories.models.Project
import org.scalacheck.Gen

object Generators {

  implicit lazy val projects: Gen[Project] = for {
    projectId <- projectIds
    path      <- projectPaths
  } yield Project(projectId, path)
}
