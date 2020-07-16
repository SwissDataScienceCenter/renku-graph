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

  override def findTopmostDerivedFrom(derivedFrom: DerivedFrom): IO[Option[DerivedFrom]] =
    queryExpecting[Set[DerivedFrom]](using = queryFindingDerivedFrom(derivedFrom)) flatMap toOption(derivedFrom)

  private def queryFindingDerivedFrom(derivedFrom: DerivedFrom) = SparqlQuery(
    name = "upload - ds topmostDerivedFrom",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>"
    ),
    s"""|SELECT ?maybeTopmostDerivedFrom
        |WHERE {
        |  <$derivedFrom> rdf:type <http://schema.org/Dataset>;
        |                 renku:topmostDerivedFrom ?maybeTopmostDerivedFrom.
        |}
        |""".stripMargin
  )

  private implicit val topmostDerivedDecoder: Decoder[Set[DerivedFrom]] = {
    val derivedFrom: Decoder[Option[DerivedFrom]] =
      _.downField("maybeTopmostDerivedFrom").downField("value").as[Option[DerivedFrom]]
    _.downField("results").downField("bindings").as(decodeList(derivedFrom)).map(_.flatten.toSet)
  }

  private def toOption[T](datasetId: T)(implicit entityTypeInfo: T => String): Set[T] => IO[Option[T]] = {
    case set if set.isEmpty   => Option.empty[T].pure[IO]
    case set if set.size == 1 => set.headOption.pure[IO]
    case _ =>
      new Exception(
        s"More than one ${entityTypeInfo(datasetId)} found for dataset $datasetId"
      ).raiseError[IO, Option[T]]
  }

  private implicit val topmostSameAsInfo:      SameAs => String      = _ => "topmostSameAs"
  private implicit val topmostDerivedFromInfo: DerivedFrom => String = _ => "topmostDerivedFrom"
}

private object IOKGDatasetInfoFinder {
  def apply(logger:              Logger[IO], timeRecorder: SparqlQueryTimeRecorder[IO])(
      implicit executionContext: ExecutionContext,
      contextShift:              ContextShift[IO],
      timer:                     Timer[IO]
  ): IO[KGDatasetInfoFinderImpl] =
    for {
      rdfStoreConfig <- RdfStoreConfig[IO]()
    } yield new KGDatasetInfoFinderImpl(rdfStoreConfig, logger, timeRecorder)
}
