/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

  def flatMapResults[F[_]: MonadThrow](f: List[Result] => F[List[Result]]): F[PagingResponse[Result]] =
    f(results) >>= {
      case r if r.size == results.size => new PagingResponse[Result](r, pagingInfo).pure[F]
      case _ => new Exception("Paging response mapping changed page size").raiseError[F, PagingResponse[Result]]
    }

  override lazy val toString: String = s"PagingResponse(pagingInfo: $pagingInfo, results: $results)"
}

object PagingResponse {

  def empty[Result](pagingRequest: PagingRequest): PagingResponse[Result] =
    new PagingResponse[Result](Nil, new PagingInfo(pagingRequest, Total(0)))

  def apply[Result](
      results:       List[Result],
      pagingRequest: PagingRequest
  ): PagingResponse[Result] = {
    val total = Total(results.size)
    create(results, pagingRequest, total)
  }

  def apply[Result](
      results:       List[Result],
      pagingRequest: PagingRequest,
      total:         Total
  ): Either[String, PagingResponse[Result]] =
    if (results.size > pagingRequest.perPage.value)
      Left(
        s"PagingResponse cannot be instantiated for ${results.size} results, total: $total, page: ${pagingRequest.page} and perPage: ${pagingRequest.perPage}"
      )
    else create(results, pagingRequest, total).asRight

  def from[F[_]: MonadThrow, Result](
      results:       List[Result],
      pagingRequest: PagingRequest
  ): F[PagingResponse[Result]] = {
    val total = Total(results.size)
    results match {
      case Nil => from(results, pagingRequest, total)
      case notEmpty =>
        val page = notEmpty
          .sliding(pagingRequest.perPage.value, pagingRequest.perPage.value)
          .toList
          .get(pagingRequest.page.value - 1)
          .getOrElse(Nil)
        from(page, pagingRequest, total)
    }
  }

  def from[F[_]: MonadThrow, Result](
      results:       List[Result],
      pagingRequest: PagingRequest,
      total:         Total
  ): F[PagingResponse[Result]] =
    apply(results, pagingRequest, total).fold(
      err => new IllegalArgumentException(err).raiseError[F, PagingResponse[Result]],
      _.pure[F]
    )

  private def create[Result](
      results:       List[Result],
      pagingRequest: PagingRequest,
      total:         Total
  ): PagingResponse[Result] = {
    val (page, perPage) = (pagingRequest.page, pagingRequest.perPage)
    if (results.nonEmpty && ((page.value - 1) * perPage.value + results.size > total.value))
      new PagingResponse[Result](
        results,
        new PagingInfo(pagingRequest, Total((page.value - 1) * perPage.value + results.size))
      )
    else
      new PagingResponse[Result](results, new PagingInfo(pagingRequest, total))
  }

  final class PagingInfo private[PagingResponse] (val pagingRequest: PagingRequest, val total: Total) {
    override lazy val toString: String = s"PagingInfo(request: $pagingRequest, total: $total)"
  }

  implicit class ResponseOps[Result](response: PagingResponse[Result]) {

    import io.circe.{Encoder, Json}
    import io.circe.syntax._
    import org.http4s.{EntityEncoder, Response, Status}
    import org.http4s.circe.jsonEncoderOf

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
    ): Response[F] = Response[F](Status.Ok)
      .withEntity(response.results.asJson)
      .putHeaders(PagingHeaders.from(response).toSeq.map(Header.ToRaw.rawToRaw): _*)

    private implicit def resultsEntityEncoder[F[_]](implicit encoder: Encoder[Result]): EntityEncoder[F, Json] =
      jsonEncoderOf[F, Json]
  }
}
