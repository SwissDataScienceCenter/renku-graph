/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel}
import io.renku.http.rest.paging.model.Total

trait Paging[Result] {

  import Paging.PagedResultsFinder

  def findPage[F[_]: MonadThrow](paging: PagingRequest)(implicit
      resultsFinder: PagedResultsFinder[F, Result],
      parallel:      NonEmptyParallel[F]
  ): F[PagingResponse[Result]] = {
    import resultsFinder._
    (findResults(paging), findTotal()).parMapN { case (results, total) =>
      PagingResponse.from[F, Result](results, paging, total)
    }.flatten
  }
}

object Paging {

  trait PagedResultsFinder[F[_], Result] {
    def findResults(paging: PagingRequest): F[List[Result]]
    def findTotal(): F[Total]
  }
}
