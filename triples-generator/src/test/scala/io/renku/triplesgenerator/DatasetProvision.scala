package io.renku.triplesgenerator

import cats.effect.IO
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.graph.model.entities.EntityFunctions
import io.renku.graph.model.{RenkuUrl, entities}
import io.renku.projectauth.{ProjectMember, Role}
import io.renku.triplesgenerator.events.consumers.{ProjectAuthSync, ProjectSparqlClient}
import io.renku.triplesstore.{GraphsProducer, InMemoryJena, ProjectsDataset}

trait DatasetProvision extends SearchInfoDatasets { self: ProjectsDataset with InMemoryJena =>

  override def provisionProject(
      project: entities.Project
  )(implicit
      entityFunctions: EntityFunctions[entities.Project],
      graphsProducer:  GraphsProducer[entities.Project],
      renkuUrl:        RenkuUrl
  ): IO[Unit] = {
    val ps      = ProjectSparqlClient[IO](projectsDSConnectionInfo).map(ProjectAuthSync[IO](_))
    val members = project.members.flatMap(p => p.maybeGitLabId.map(id => ProjectMember(id, Role.Reader)))
    super.provisionProject(project) *> ps.use(_.syncProject(project.slug, members))
  }
}
