/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.http.rest.paging

import cats.MonadThrow
import cats.syntax.all._
import io.renku.http.rest.paging.model.Total

trait Paging[Result] {

  import Paging.PagedResultsFinder

  def findPage[Interpretation[_]: MonadThrow](paging: PagingRequest)(implicit
      resultsFinder: PagedResultsFinder[Interpretation, Result]
  ): Interpretation[PagingResponse[Result]] = for {
    results  <- resultsFinder.findResults(paging)
    response <- prepareResponse(results, paging)
  } yield response

  private def prepareResponse[Interpretation[_]: MonadThrow](
      results:              List[Result],
      pagingRequest:        PagingRequest
  )(implicit resultsFinder: PagedResultsFinder[Interpretation, Result]) = for {
    total <- if (results.nonEmpty && results.size < pagingRequest.perPage.value)
               Total((pagingRequest.page.value - 1) * pagingRequest.perPage.value + results.size).pure[Interpretation]
             else
               resultsFinder.findTotal()
    response <- PagingResponse.from[Interpretation, Result](results, pagingRequest, total)
  } yield response
}

object Paging {

  trait PagedResultsFinder[Interpretation[_], Result] {
    def findResults(paging: PagingRequest): Interpretation[List[Result]]
    def findTotal(): Interpretation[Total]
  }
}
