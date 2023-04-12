package io.renku.entities.search

import io.renku.entities.search.model.Entity
import io.renku.http.rest.paging.PagingResponse

trait RecentEnitiesFinder[F[_]] {

  def findRecentlyViewedEntities(limit: Int): F[PagingResponse[Entity]]

}
