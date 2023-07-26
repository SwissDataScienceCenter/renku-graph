package io.renku.projectauth

import fs2.Pipe
import io.renku.graph.model.{RenkuUrl, Schemas}
import io.renku.jsonld.NamedGraph
import io.renku.jsonld.syntax._
import io.renku.projectauth.sparql.SparqlClient

/** Manage authorization data for projects and members. */
trait ProjectAuthService[F[_]] {

  def update(data: ProjectAuthData): F[Unit]

  def updateAll: Pipe[F, ProjectAuthData, Nothing]

}

object ProjectAuthService {

  def apply[F[_]](client: SparqlClient[F], renkuUrl: RenkuUrl): ProjectAuthService[F] =
    new Impl[F](client)(renkuUrl)

  private final class Impl[F[_]](sparqlClient: SparqlClient[F])(implicit renkuUrl: RenkuUrl)
      extends ProjectAuthService[F] {
    private[this] val graph = Schemas.renku / "ProjectAuth"

    override def update(data: ProjectAuthData): F[Unit] = {
      val jsonld = NamedGraph.fromJsonLDsUnsafe(graph, data.asJsonLD)
      sparqlClient.upload(jsonld)
    }

    override def updateAll: Pipe[F, ProjectAuthData, Nothing] =
      _.chunks
        .map(chunk => NamedGraph.fromJsonLDsUnsafe(graph, chunk.toList.map(_.asJsonLD)))
        .evalMap(sparqlClient.upload)
        .drain
  }
}
