/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.http.rest.paging

import cats.MonadError
import cats.implicits._
import ch.datascience.http.rest.paging.model.Total

import scala.language.higherKinds

trait Paging[Interpretation[_], Result] {

  import Paging.PagedResultsFinder

  def findPage(paging:        PagingRequest)(
      implicit resultsFinder: PagedResultsFinder[Interpretation, Result],
      ME:                     MonadError[Interpretation, Throwable]
  ): Interpretation[PagingResponse[Result]] =
    for {
      results  <- resultsFinder.findResults(paging)
      response <- prepareResponse(results, paging)
    } yield response

  private def prepareResponse(
      results:              List[Result],
      paging:               PagingRequest
  )(implicit resultsFinder: PagedResultsFinder[Interpretation, Result], ME: MonadError[Interpretation, Throwable]) =
    if (results.nonEmpty && results.size < paging.perPage.value)
      PagingResponse(
        results,
        PagingInfo(paging.page, paging.perPage, Total((paging.page.value - 1) * paging.perPage.value + results.size))
      ).pure[Interpretation]
    else
      resultsFinder.findTotal() map (
          total => PagingResponse(results, PagingInfo(paging.page, paging.perPage, total))
      )
}

object Paging {

  trait PagedResultsFinder[Interpretation[_], Result] {
    def findResults(paging: PagingRequest): Interpretation[List[Result]]
    def findTotal(): Interpretation[Total]
  }
}
