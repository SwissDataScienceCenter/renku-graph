package ch.datascience.knowledgegraph.datasets

import ch.datascience.generators.CommonGraphGenerators.cliVersions
import ch.datascience.generators.Generators.nonEmptyStrings
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.datasets.{Identifier, TopmostDerivedFrom}
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators.addedToProjectObjects
import ch.datascience.knowledgegraph.datasets.model.DatasetProject
import ch.datascience.rdfstore.entities.Person.persons
import ch.datascience.rdfstore.entities.{Activity, Agent, Artifact, Entity, Location, Project}
import org.scalacheck.Gen

object EntityGenerators {
  implicit class ProjectOps(project: Project) {
    lazy val toDatasetProject: DatasetProject =
      DatasetProject(project.path, project.name, addedToProjectObjects.generateOne)
  }

  private def activities(project: Project): Gen[Activity] = for {
    commitId      <- commitIds
    committedDate <- committedDates
    committer     <- persons
    cliVersion    <- cliVersions
    comment       <- nonEmptyStrings()
  } yield Activity(
    commitId,
    committedDate,
    committer,
    project,
    Agent(cliVersion),
    comment,
    None,
    None,
    None,
    Nil
  )

  def invalidationEntity(datasetId:          Identifier,
                         project:            Project,
                         topmostDerivedFrom: Option[TopmostDerivedFrom] = None
  ): Gen[Entity with Artifact] = for {
    activity <- activities(project)
  } yield new Entity(
    activity.commitId,
    Location(".renku") / "datasets" / topmostDerivedFrom.map(_.value).getOrElse(datasetId.value) / "metadata.yml",
    project,
    Some(activity),
    None
  ) with Artifact
}
