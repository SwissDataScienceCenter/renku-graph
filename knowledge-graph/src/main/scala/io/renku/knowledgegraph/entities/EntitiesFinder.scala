package io.renku.knowledgegraph.entities

import cats.effect.Async
import io.renku.rdfstore.{RdfStoreClientImpl, RdfStoreConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private trait EntitiesFinder[F[_]] {
  def findEntities(): F[List[Entity]]
}

private class EntitiesFinderImpl[F[_]: Async: Logger](
    rdfStoreConfig: RdfStoreConfig,
    timeRecorder:   SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl[F](rdfStoreConfig, timeRecorder)
    with EntitiesFinder[F] {

  override def findEntities(): F[List[Entity]] = ???
}
