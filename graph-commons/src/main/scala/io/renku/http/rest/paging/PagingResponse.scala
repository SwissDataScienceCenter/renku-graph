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

import cats.MonadThrow
import cats.syntax.all._
import io.renku.http.rest.paging.PagingResponse.PagingInfo
import io.renku.http.rest.paging.model.Total
import io.renku.tinytypes.UrlTinyType
import io.renku.tinytypes.constraints.UrlOps
import org.http4s.Header

final class PagingResponse[Result] private (val results: List[Result], val pagingInfo: PagingInfo) {
  override lazy val toString: String = s"PagingResponse(pagingInfo: $pagingInfo, results: $results)"
}

object PagingResponse {

  def empty[Result](pagingRequest: PagingRequest): PagingResponse[Result] =
    new PagingResponse[Result](Nil, new PagingInfo(pagingRequest, Total(0)))

  def from[F[_]: MonadThrow, Result](
      results:       List[Result],
      pagingRequest: PagingRequest,
      total:         Total
  ): F[PagingResponse[Result]] = {

    val pagingInfo = new PagingInfo(pagingRequest, total)

    import pagingRequest._

    if (results.isEmpty && (page.value - 1) * perPage.value >= total.value) {
      new PagingResponse[Result](results, pagingInfo).pure[F]
    } else if (results.nonEmpty && ((page.value - 1) * perPage.value + results.size) == total.value) {
      new PagingResponse[Result](results, pagingInfo).pure[F]
    } else if (results.nonEmpty && (results.size == perPage.value) && (page.value * perPage.value) <= total.value) {
      new PagingResponse[Result](results, pagingInfo).pure[F]
    } else
      new IllegalArgumentException(
        s"PagingResponse cannot be instantiated for ${results.size} results, total: $total, page: ${pagingRequest.page} and perPage: ${pagingRequest.perPage}"
      ).raiseError[F, PagingResponse[Result]]
  }

  final class PagingInfo private[PagingResponse] (val pagingRequest: PagingRequest, val total: Total) {
    override lazy val toString: String = s"PagingInfo(request: $pagingRequest, total: $total)"
  }

  implicit class ResponseOps[Result](response: PagingResponse[Result]) {

    import io.circe.syntax._
    import io.circe.{Encoder, Json}
    import org.http4s.circe.jsonEncoderOf
    import org.http4s.{EntityEncoder, Response, Status}

    def updateResults[F[_]: MonadThrow](newResults: List[Result]): F[PagingResponse[Result]] =
      if (response.results.size == newResults.size)
        new PagingResponse[Result](newResults, response.pagingInfo).pure[F]
      else
        new IllegalArgumentException("Cannot update Paging Results as there's different number of results")
          .raiseError[F, PagingResponse[Result]]

    def toHttpResponse[F[_], ResourceUrl <: UrlTinyType](implicit
        resourceUrl:    ResourceUrl,
        resourceUrlOps: UrlOps[ResourceUrl],
        encoder:        Encoder[Result]
    ): Response[F] =
      Response[F](Status.Ok)
        .withEntity(response.results.asJson)
        .putHeaders(PagingHeaders.from(response).toSeq.map(Header.ToRaw.rawToRaw): _*)

    private implicit def resultsEntityEncoder[F[_]](implicit encoder: Encoder[Result]): EntityEncoder[F, Json] =
      jsonEncoderOf[F, Json]
  }
}
