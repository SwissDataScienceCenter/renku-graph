package ch.datascience.triplesgenerator.reprovisioning

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.graph.Schemas.rdf
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.models.RenkuVersionPair
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

trait RenkuVersionPairFinder[Interpretation[_]] {

  def find(): Interpretation[Option[RenkuVersionPair]]

}

private class IORenkuVersionPairFinder(rdfStoreConfig: RdfStoreConfig,
                                       renkuBaseUrl:   RenkuBaseUrl,
                                       logger:         Logger[IO],
                                       timeRecorder:   SparqlQueryTimeRecorder[IO]
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO],
    ME:               MonadError[IO, Throwable]
) extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with RenkuVersionPairFinder[IO] {

  override def find(): IO[Option[RenkuVersionPair]] = queryExpecting[List[RenkuVersionPair]] {
    val entityId = (renkuBaseUrl / "version-pair").showAs[RdfResource]
    SparqlQuery.of(
      name = "version pair find",
      Prefixes.of(rdf -> "rdf"),
      s"""|SELECT DISTINCT ?schemaVersion ?cliVersion
          |WHERE {
          |  $entityId rdf:type <${RenkuVersionPairJsonLD.objectType}>;
          |      <${RenkuVersionPairJsonLD.schemaVersion}> ?schemaVersion;
          |      <${RenkuVersionPairJsonLD.cliVersion}> ?cliVersion.
          |}
          |""".stripMargin
    )
  }.flatMap {
    case Nil         => None.pure[IO]
    case head :: Nil => head.some.pure[IO]
    case versionPairs =>
      new IllegalStateException(s"Too many Version pair found: $versionPairs").raiseError[IO, Option[RenkuVersionPair]]
  }
}
