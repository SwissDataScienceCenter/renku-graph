package io.renku.knowledgegraph.datasets

import cats.effect.kernel.Sync
import fs2.Stream
import io.renku.graph.http.server.security.Authorizer
import io.renku.graph.model.datasets
import io.renku.http.server.security.model
import io.renku.jsonld.EntityId
import io.renku.projectauth.util.ProjectAuthDataRow
import io.renku.triplesstore.ProjectSparqlClient
import io.renku.triplesstore.client.syntax._

trait DatasetSameAsRecordsFinder2[F[_]] extends Authorizer.SecurityRecordFinder[F, datasets.SameAs]

object DatasetSameAsRecordsFinder2 {

  def apply[F[_]: Sync](projectSparqlClient: ProjectSparqlClient[F]): DatasetSameAsRecordsFinder2[F] =
    new Impl[F](projectSparqlClient)

  private class Impl[F[_]: Sync](projectSparqlClient: ProjectSparqlClient[F]) extends DatasetSameAsRecordsFinder2[F] {
    override def apply(id: datasets.SameAs, user: Option[model.AuthUser]): F[List[Authorizer.SecurityRecord]] =
      Stream
        .evals(projectSparqlClient.queryDecode[ProjectAuthDataRow](query(id)))
        .through(ProjectAuthDataRow.collect)
        .map(p => Authorizer.SecurityRecord(p.visibility, p.slug, p.members.map(_.gitLabId)))
        .compile
        .toList

    private def query(sameAs: datasets.SameAs) =
      sparql"""PREFIX schema: <http://schema.org/>
              |PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>
              |
              |  select ?slug ?visibility ?memberRole
              |  where {
              |    bind (${EntityId.of(sameAs.value)} as ?dsSameAs).
              |    graph schema:Dataset {
              |      ?dsSameAs a renku:DiscoverableDataset;
              |                renku:datasetProjectLink / renku:project ?projectId.
              |    }
              |    graph renku:ProjectAuth {
              |      ?projectId a schema:Project;
              |                 renku:slug ?slug;
              |                 renku:visibility ?visibility.
              |      Optional {
              |        ?projectId renku:memberRole ?memberRole.
              |      }
              |    }
              |  }
              |  ORDER BY ?slug
              |""".stripMargin
  }
}
