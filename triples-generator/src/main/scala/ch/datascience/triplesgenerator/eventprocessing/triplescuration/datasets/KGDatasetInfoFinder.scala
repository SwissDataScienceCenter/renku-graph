package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.model.datasets.{DerivedFrom, IdSameAs, SameAs}
import ch.datascience.rdfstore._
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder
import io.circe.Decoder.decodeList

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait KGDatasetInfoFinder[Interpretation[_]] {
  def findTopmostSameAs(idSameAs:         IdSameAs):    Interpretation[Option[SameAs]]
  def findTopmostDerivedFrom(derivedFrom: DerivedFrom): Interpretation[Option[DerivedFrom]]
}

private class KGDatasetInfoFinderImpl(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with KGDatasetInfoFinder[IO] {

  import cats.implicits._
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import eu.timepit.refined.auto._

  override def findTopmostSameAs(sameAs: IdSameAs): IO[Option[SameAs]] =
    queryExpecting[Set[SameAs]](using = queryFindingSameAs(sameAs)) flatMap toOption(sameAs)

  private def queryFindingSameAs(sameAs: IdSameAs) = SparqlQuery(
    name = "upload - ds topmostSameAs",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT ?maybeTopmostSameAs
        |WHERE {
        |  <$sameAs> rdf:type <http://schema.org/Dataset>;
        |            renku:topmostSameAs/schema:url ?maybeTopmostSameAs.
        |}
        |""".stripMargin
  )

  private implicit val topmostSameAsDecoder: Decoder[Set[SameAs]] = {
    val sameAs: Decoder[Option[SameAs]] = _.downField("maybeTopmostSameAs").downField("value").as[Option[SameAs]]
    _.downField("results").downField("bindings").as(decodeList(sameAs)).map(_.flatten.toSet)
  }

  override def findTopmostDerivedFrom(derivedFrom: DerivedFrom): IO[Option[DerivedFrom]] = ???

  private def toOption(sameAs: SameAs): Set[SameAs] => IO[Option[SameAs]] = {
    case set if set.isEmpty   => Option.empty[SameAs].pure[IO]
    case set if set.size == 1 => set.headOption.pure[IO]
    case _                    => new Exception(s"More than one topmostSame found for dataset $sameAs").raiseError[IO, Option[SameAs]]
  }
}
