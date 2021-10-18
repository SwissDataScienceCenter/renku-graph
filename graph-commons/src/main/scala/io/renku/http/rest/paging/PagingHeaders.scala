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

import io.renku.http.rest.paging.PagingRequest.Decoders.page.{parameterName => pageParamName}
import io.renku.http.rest.paging.PagingResponse.PagingInfo
import io.renku.http.rest.paging.model.Page.first
import io.renku.tinytypes.UrlTinyType
import io.renku.tinytypes.constraints.UrlOps
import org.http4s.Header

object PagingHeaders {

  val Total:      String = "Total"
  val TotalPages: String = "Total-Pages"
  val PerPage:    String = "Per-Page"
  val Page:       String = "Page"
  val NextPage:   String = "Next-Page"
  val PrevPage:   String = "Prev-Page"
  val Link:       String = "Link"

  def from[F[_], ResourceUrl <: UrlTinyType](response: PagingResponse[_])(implicit
      resourceUrl:                                     ResourceUrl,
      resourceUrlOps:                                  UrlOps[ResourceUrl]
  ): Set[Header] =
    Set(
      Some(Header(Total, response.pagingInfo.total.toString)),
      Some(Header(TotalPages, totalPages(response.pagingInfo).toString)),
      Some(Header(PerPage, response.pagingInfo.pagingRequest.perPage.toString)),
      Some(Header(Page, response.pagingInfo.pagingRequest.page.toString)),
      nextPage(response.pagingInfo),
      prevPage(response.pagingInfo),
      nextLink(response.pagingInfo),
      prevLink(response.pagingInfo),
      firstLink,
      lastLink(response.pagingInfo)
    ).flatten

  private def totalPages(pagingInfo: PagingInfo): Int = {
    val pages = pagingInfo.total.value / pagingInfo.pagingRequest.perPage.value.toFloat
    if (pages.isWhole) pages.toInt else (pages + 1).toInt
  }

  private def nextPage(pagingInfo: PagingInfo): Option[Header] =
    if (pagingInfo.pagingRequest.page.value == totalPages(pagingInfo) || pagingInfo.total.value == 0) None
    else Some(Header(NextPage, (pagingInfo.pagingRequest.page.value + 1).toString))

  private def prevPage(pagingInfo: PagingInfo): Option[Header] =
    if (pagingInfo.pagingRequest.page == first || pagingInfo.total.value == 0) None
    else Some(Header(PrevPage, (pagingInfo.pagingRequest.page.value - 1).toString))

  private def prevLink[F[_], ResourceUrl <: UrlTinyType](
      pagingInfo:         PagingInfo
  )(implicit resourceUrl: ResourceUrl, resourceUrlOps: UrlOps[ResourceUrl]): Option[Header] = {
    val page = pagingInfo.pagingRequest.page
    if (page == first || pagingInfo.total.value == 0) None
    else Some(Header(Link, s"""<${uriWithPageParam(pageParamName, page.value - 1)}>; rel="prev""""))
  }

  private def uriWithPageParam[F[_], ResourceUrl <: UrlTinyType](
      paramName:          String,
      value:              Int
  )(implicit resourceUrl: ResourceUrl, resourceUrlOps: UrlOps[ResourceUrl]) = {
    import resourceUrlOps._
    resourceUrl ? (paramName -> value)
  }

  private def nextLink[F[_], ResourceUrl <: UrlTinyType](
      pagingInfo:         PagingInfo
  )(implicit resourceUrl: ResourceUrl, resourceUrlOps: UrlOps[ResourceUrl]): Option[Header] = {
    val page = pagingInfo.pagingRequest.page
    if (page.value == totalPages(pagingInfo) || pagingInfo.total.value == 0) None
    else Some(Header(Link, s"""<${uriWithPageParam(pageParamName, page.value + 1)}>; rel="next""""))
  }

  private def firstLink[F[_], ResourceUrl <: UrlTinyType](implicit
      resourceUrl:    ResourceUrl,
      resourceUrlOps: UrlOps[ResourceUrl]
  ): Option[Header] =
    Some(Header(Link, s"""<${uriWithPageParam(pageParamName, 1)}>; rel="first""""))

  private def lastLink[F[_], ResourceUrl <: UrlTinyType](pagingInfo: PagingInfo)(implicit
      resourceUrl:                                                   ResourceUrl,
      resourceUrlOps:                                                UrlOps[ResourceUrl]
  ): Option[Header] = if (pagingInfo.total.value > 0) {
    Some(Header(Link, s"""<${uriWithPageParam(pageParamName, totalPages(pagingInfo))}>; rel="last""""))
  } else Some(Header(Link, s"""<${uriWithPageParam(pageParamName, 1)}>; rel="last""""))
}
