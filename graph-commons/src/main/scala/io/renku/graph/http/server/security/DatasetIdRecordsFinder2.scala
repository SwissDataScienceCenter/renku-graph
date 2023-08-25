package io.renku.graph.http.server.security

import cats.effect.kernel.Sync
import io.renku.graph.http.server.security.Authorizer.SecurityRecordFinder
import io.renku.graph.model.datasets
import io.renku.http.server.security.model
import io.renku.triplesstore.ProjectSparqlClient
import io.renku.triplesstore.client.http.RowDecoder
import io.renku.triplesstore.client.syntax._

trait DatasetIdRecordsFinder2[F[_]] extends SecurityRecordFinder[F, datasets.Identifier]

object DatasetIdRecordsFinder2 {

  private class Impl[F[_]: Sync](projectSparqlClient: ProjectSparqlClient[F]) extends DatasetIdRecordsFinder2[F] {
    override def apply(id: datasets.Identifier, user: Option[model.AuthUser]): F[List[Authorizer.SecurityRecord]] =
      projectSparqlClient.queryDecode[Authorizer.SecurityRecord](query(id))

    private def query(id: datasets.Identifier) =
      sparql"""
              |""".stripMargin

    implicit val rowDecoder: RowDecoder[Authorizer.SecurityRecord] = ???
  }
}
