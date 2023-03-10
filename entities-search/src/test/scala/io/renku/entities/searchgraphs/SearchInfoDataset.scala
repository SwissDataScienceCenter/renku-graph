package io.renku.entities.searchgraphs

import cats.effect.IO
import cats.syntax.all._
import io.renku.graph.model.{entities, testentities, RenkuUrl}
import io.renku.graph.model.entities.EntityFunctions
import io.renku.logging.{ExecutionTimeRecorder, TestExecutionTimeRecorder}
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

trait SearchInfoDataset {
  self: ProjectsDataset with InMemoryJena =>

  implicit def ioLogger: Logger[IO]

  def provisionTestProjects(
      projects: testentities.Project*
  )(implicit
      renkuUrl:        RenkuUrl,
      entityFunctions: EntityFunctions[entities.Project],
      graphsProducer:  GraphsProducer[entities.Project]
  ) = projects.map(provisionTestProject).sequence

  def provisionTestProject(
      p: testentities.Project
  )(implicit
      renkuUrl:        RenkuUrl,
      entityFunctions: EntityFunctions[entities.Project],
      graphsProducer:  GraphsProducer[entities.Project]
  ) =
    provisionProject(p.to[entities.Project])

  def provisionProject(
      project: entities.Project
  )(implicit entityFunctions: EntityFunctions[entities.Project], graphsProducer: GraphsProducer[entities.Project]) =
    uploadIO(projectsDataset, graphsProducer(project): _*) >> insertSearchInfo(project)

  def insertSearchInfo(project: entities.Project): IO[Unit] =
    createDatasetsGraphProvisioner.flatMap(_.provisionDatasetsGraph(project))

  def createDatasetsGraphProvisioner: IO[DatasetsGraphProvisioner[IO]] = {
    val execTimeRecorder: ExecutionTimeRecorder[IO] = TestExecutionTimeRecorder[IO]()
    implicit val sparqlQueryTimeRecorder: SparqlQueryTimeRecorder[IO] =
      new SparqlQueryTimeRecorder[IO](execTimeRecorder)

    IO(DatasetsGraphProvisioner[IO](projectsDSConnectionInfo))
  }
}
