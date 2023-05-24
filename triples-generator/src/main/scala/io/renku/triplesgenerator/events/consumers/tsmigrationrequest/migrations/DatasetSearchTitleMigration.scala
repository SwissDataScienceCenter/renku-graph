package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.data.EitherT
import cats.effect.Async
import eu.timepit.refined.auto._
import fs2.Stream
import io.circe.Decoder
import io.renku.graph.model.{GraphClass, Schemas, datasets}
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.{MigrationExecutionRegister, RecoverableErrorsRecovery, RegisteredMigration}
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax.FragmentStringContext
import io.renku.triplesstore.{SparqlQuery, TSClient}
import org.typelevel.log4cats.Logger

private class DatasetSearchTitleMigration[F[_]: Async: Logger](
    tsClient:          TSClient[F],
    executionRegister: MigrationExecutionRegister[F],
    recoveryStrategy:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends RegisteredMigration[F](ProjectsDateViewedCreator.name, executionRegister, recoveryStrategy) {
  val chunkSize = 100
  protected override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = ???


  def queryAll: Stream[F, DatasetSearchTitleMigration.DsTitle] =
    Stream
      .iterate(0)(_ + chunkSize)
      .covary[F]
      .evalMap(sz => tsClient.queryExpecting[List[DatasetSearchTitleMigration.DsTitle]](query(chunkSize, sz)))
      .flatMap(el => fs2.Stream.emits(el))

  def query(limit: Int, offset: Int) = SparqlQuery.of(
    "dataset title query",
    Prefixes.of(Schemas.renku -> "renku"),
    sparql"""
            |SELECT DISTINCT ?sameAs ?title
            |WHERE {
            |  Graph ?id {
            |    ?id a schema:Project.
            |    ?dsId a schema:Dataset;
            |          renku:topmostSameAs ?sameAs;
            |          schema:identifier ?identifier;
            |          schema:name ?title.
            |  }
            |}
            |ORDER BY ?sameAs ?title
            |LIMIT $limit
            |OFFSET $offset
            |""".stripMargin
  )
}

object DatasetSearchTitleMigration {
  final case class DsTitle(sameAs: datasets.TopmostSameAs, title: datasets.Title)

  object DsTitle {
    implicit def decoder: Decoder[DsTitle] = ???
  }
}
