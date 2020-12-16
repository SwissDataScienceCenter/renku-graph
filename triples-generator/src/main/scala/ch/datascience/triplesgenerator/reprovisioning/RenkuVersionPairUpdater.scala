package ch.datascience.triplesgenerator.reprovisioning

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.Schemas._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery.{Prefix, Prefixes}
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.models.RenkuVersionPair
import io.chrisdavenport.log4cats.Logger
import io.renku.jsonld.EntityId
import eu.timepit.refined.auto._

import scala.concurrent.ExecutionContext

trait RenkuVersionPairUpdater[Interpretation[_]] {

  def update(versionPair: RenkuVersionPair): Interpretation[Unit]
}

private case object RenkuVersionPairJsonLD {
  import ch.datascience.graph.Schemas._

  def id(implicit renkuBaseUrl: RenkuBaseUrl) = EntityId.of((renkuBaseUrl / "version-pair").toString)
  val objectType    = renku / "VersionPair"
  val cliVersion    = renku / "cliVersion"
  val schemaVersion = renku / "schemaVersion"
}

private class IORenkuVersionPairUpdater(rdfStoreConfig: RdfStoreConfig,
                                        renkuBaseUrl:   RenkuBaseUrl,
                                        logger:         Logger[IO],
                                        timeRecorder:   SparqlQueryTimeRecorder[IO]
)(implicit executionContext:                            ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with RenkuVersionPairUpdater[IO] {
  override def update(versionPair: RenkuVersionPair): IO[Unit] = updateWithNoResult {
    val entityId = (renkuBaseUrl / "version-pair").showAs[RdfResource]
    SparqlQuery.of(
      name = "reprovisioning - cli and schema version create",
      Prefixes.of(
        rdf   -> "rdf",
        renku -> "renku"
      ),
      s"""|DELETE {$entityId <${RenkuVersionPairJsonLD.cliVersion}> ?o .
          |        $entityId <${RenkuVersionPairJsonLD.schemaVersion}> ?q .
          |}
          |
          |INSERT { 
          |  <${RenkuVersionPairJsonLD.id(renkuBaseUrl)}> rdf:type <${RenkuVersionPairJsonLD.objectType}> ;
          |                                         <${RenkuVersionPairJsonLD.cliVersion}> '${versionPair.cliVersion}' ;
          |                                          <${RenkuVersionPairJsonLD.schemaVersion}> '${versionPair.schemaVersion}'
          |}
          |WHERE {
          |  OPTIONAL {
          |    $entityId ?p ?o;
          |              ?r ?q.
          |  }
          |}
          |""".stripMargin
    )
  }
}
