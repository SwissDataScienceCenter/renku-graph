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

package ch.datascience.http.rest.paging

import cats.MonadError
import cats.syntax.all._
import ch.datascience.config.renku
import ch.datascience.http.rest.paging.PagingResponse.PagingInfo
import ch.datascience.http.rest.paging.model.Total

final class PagingResponse[Result] private (val results: List[Result], val pagingInfo: PagingInfo) {
  override lazy val toString: String = s"PagingResponse(pagingInfo: $pagingInfo, results: $results)"
}

object PagingResponse {

  final class PagingInfo private[PagingResponse] (val pagingRequest: PagingRequest, val total: Total) {
    override lazy val toString: String = s"PagingInfo(request: $pagingRequest, total: $total)"
  }

  def from[Interpretation[_], Result](
      results:       List[Result],
      pagingRequest: PagingRequest,
      total:         Total
  )(implicit ME:     MonadError[Interpretation, Throwable]): Interpretation[PagingResponse[Result]] = {

    val pagingInfo = new PagingInfo(pagingRequest, total)

    import pagingRequest._

    if (results.isEmpty && (page.value - 1) * perPage.value >= total.value) {
      new PagingResponse[Result](results, pagingInfo).pure[Interpretation]
    } else if (results.nonEmpty && ((page.value - 1) * perPage.value + results.size) == total.value) {
      new PagingResponse[Result](results, pagingInfo).pure[Interpretation]
    } else if (results.nonEmpty && (results.size == perPage.value) && (page.value * perPage.value) <= total.value) {
      new PagingResponse[Result](results, pagingInfo).pure[Interpretation]
    } else
      new IllegalArgumentException(
        s"PagingResponse cannot be instantiated for ${results.size} results, total: $total, page: ${pagingRequest.page} and perPage: ${pagingRequest.perPage}"
      ).raiseError[Interpretation, PagingResponse[Result]]
  }

  implicit class ResponseOps[Result](response: PagingResponse[Result]) {

    import cats.effect.Effect
    import io.circe.syntax._
    import io.circe.{Encoder, Json}
    import org.http4s.circe.jsonEncoderOf
    import org.http4s.{EntityEncoder, Response, Status}

    def updateResults[Interpretation[_]](
        newResults: List[Result]
    )(implicit ME:  MonadError[Interpretation, Throwable]): Interpretation[PagingResponse[Result]] =
      if (response.results.size == newResults.size)
        new PagingResponse[Result](newResults, response.pagingInfo).pure[Interpretation]
      else
        new IllegalArgumentException("Cannot update Paging Results as there's different number of results")
          .raiseError[Interpretation, PagingResponse[Result]]

    def toHttpResponse[Interpretation[_]: Effect](implicit
        renkuResourceUrl: renku.ResourceUrl,
        encoder:          Encoder[Result]
    ): Response[Interpretation] =
      Response(Status.Ok)
        .withEntity(response.results.asJson)
        .putHeaders(PagingHeaders.from(response).toSeq: _*)

    private implicit def resultsEntityEncoder[Interpretation[_]: Effect](implicit
        encoder: Encoder[Result]
    ): EntityEncoder[Interpretation, Json] = jsonEncoderOf[Interpretation, Json]
  }
}
