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

import ch.datascience.http.rest.paging.PagingResponse.PagingInfo
import ch.datascience.http.rest.paging.model.Page.first
import org.http4s.Header

object PagingHeaders {

  val Total:      String = "Total"
  val TotalPages: String = "Total-Pages"
  val PerPage:    String = "Per-Page"
  val Page:       String = "Page"
  val NextPage:   String = "Next-Page"
  val PrevPage:   String = "Prev-Page"

  def from(response: PagingResponse[_]): Set[Header] =
    Set(
      Some(Header(Total, response.pagingInfo.total.toString)),
      Some(Header(TotalPages, totalPages(response.pagingInfo).toString)),
      Some(Header(PerPage, response.pagingInfo.pagingRequest.perPage.toString)),
      Some(Header(Page, response.pagingInfo.pagingRequest.page.toString)),
      nextPage(response.pagingInfo),
      prevPage(response.pagingInfo)
    ).flatten

  private def totalPages(pagingInfo: PagingInfo): Int = {
    val pages = pagingInfo.total.value / pagingInfo.pagingRequest.perPage.value.toFloat
    if (pages.isWhole()) pages.toInt else (pages + 1).toInt
  }

  private def nextPage(pagingInfo: PagingInfo): Option[Header] =
    if (pagingInfo.pagingRequest.page.value == totalPages(pagingInfo)) None
    else Some(Header(NextPage, (pagingInfo.pagingRequest.page.value + 1).toString))

  private def prevPage(pagingInfo: PagingInfo): Option[Header] =
    if (pagingInfo.pagingRequest.page == first) None
    else Some(Header(PrevPage, (pagingInfo.pagingRequest.page.value - 1).toString))
}
