package ch.datascience.rdfstore.entities

import ch.datascience.graph.model.GraphModelGenerators._
import org.scalacheck.Gen
import Person.persons
import ch.datascience.generators.Generators.Implicits.GenOps

object ProjectsGenerators {
  implicit val projects: Gen[Project] = for {
    path         <- projectPaths
    name         <- projectNames
    dateCreated  <- projectCreatedDates
    maybeCreator <- persons.toGeneratorOfOptions
    version      <- projectSchemaVersions
  } yield Project(path, name, dateCreated, maybeCreator, maybeParentProject = None, version)
}
