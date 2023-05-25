package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.{GraphClass, Schemas}
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.Migration
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.UpdateQueryMigration
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{SparqlQuery, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private object DatasetSearchTitleMigration {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[Migration[F]] =
    UpdateQueryMigration[F](name, query).widen

  private lazy val name = Migration.Name("Insert dataset title into the dataset search graph")

  private[migrations] lazy val query = SparqlQuery.of(
    name.asRefined,
    Prefixes.of(Schemas.schema -> "schema", Schemas.renku -> "renku"),
    sparql"""|INSERT {
             |  Graph ${GraphClass.Datasets.id} {
             |    ?sameAs schema:name ?title
             |  }
             |}
             |WHERE {
             |  Graph ${GraphClass.Datasets.id} {
             |    ?sameAs a renku:DiscoverableDataset;
             |       renku:datasetProjectLink ?link.
             |
             |    ?link renku:project ?projectId;
             |          renku:dataset ?dsId
             |  }
             |
             |  Graph ?projectId {
             |    ?dsId schema:name ?title
             |  }
             |}
             |""".stripMargin
  )
}
