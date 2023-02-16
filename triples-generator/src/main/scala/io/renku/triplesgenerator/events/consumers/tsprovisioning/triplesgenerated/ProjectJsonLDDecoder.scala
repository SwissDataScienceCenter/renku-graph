package io.renku.triplesgenerator.events.consumers.tsprovisioning.triplesgenerated

import cats.syntax.all._
import io.renku.cli.model.CliProject
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.entities.Project
import io.renku.graph.model.entities.Project.GitLabProjectInfo
import io.renku.jsonld.JsonLDDecoder

trait ProjectJsonLDDecoder {

  def apply(gitLabInfo: GitLabProjectInfo)(implicit renkuUrl: RenkuUrl): JsonLDDecoder[Project] =
    CliProject.projectAndPersonDecoder.emap { case (project, persons) =>
      Project.fromCli(project, persons.toSet, gitLabInfo).toEither.leftMap(_.intercalate("; "))
    }

  def list(gitLabInfo: GitLabProjectInfo)(implicit renkuUrl: RenkuUrl): JsonLDDecoder[List[Project]] =
    JsonLDDecoder.decodeList(apply(gitLabInfo))
}

object ProjectJsonLDDecoder extends ProjectJsonLDDecoder
