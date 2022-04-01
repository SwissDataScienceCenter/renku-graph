package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.tooling

import cats.effect.Async
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.graph.model.projects
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait RecordsFinder[F[_]] {
  def findRecords(): F[List[projects.Path]]
}

private class RecordsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](query: SparqlQuery,
                                                                              rdfStoreConfig: RdfStoreConfig
) extends RdfStoreClientImpl[F](rdfStoreConfig)
    with RecordsFinder[F] {

  override def findRecords(): F[List[projects.Path]] = queryExpecting[List[projects.Path]](query)

  implicit lazy val pathsDecoder: Decoder[List[projects.Path]] = { topCursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._
    val renkuVersionPairs: Decoder[projects.Path] = _.downField("path").downField("value").as[projects.Path]
    topCursor.downField("results").downField("bindings").as(decodeList(renkuVersionPairs))
  }
}
