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

package ch.datascience.http.rest

object Paging {

  import ch.datascience.http.rest.Paging.PagingRequest._

  case class PagingRequest(page: Page, perPage: PerPage)

  object PagingRequest {
    import cats.data._
    import cats.implicits._
    import ch.datascience.tinytypes.constraints.PositiveInt
    import ch.datascience.tinytypes.{IntTinyType, TinyTypeFactory}
    import org.http4s.dsl.impl.OptionalValidatingQueryParamDecoderMatcher
    import org.http4s.{ParseFailure, QueryParamDecoder}

    val default: PagingRequest = PagingRequest(Page.first, PerPage.default)

    def apply(maybePage:    Option[ValidatedNel[ParseFailure, Page]],
              maybePerPage: Option[ValidatedNel[ParseFailure, PerPage]]): ValidatedNel[ParseFailure, PagingRequest] =
      (maybePage getOrElse Page.first.validNel, maybePerPage getOrElse PerPage.default.validNel)
        .mapN(PagingRequest.apply)

    final class Page private (val value: Int) extends AnyVal with IntTinyType
    implicit object Page extends TinyTypeFactory[Page](new Page(_)) with PositiveInt {
      val first: Page = Page(1)
    }

    final class PerPage private (val value: Int) extends AnyVal with IntTinyType
    implicit object PerPage extends TinyTypeFactory[PerPage](new PerPage(_)) with PositiveInt {
      val default: PerPage = PerPage(20)
    }

    object Decoders {
      private implicit val pageParameterDecoder: QueryParamDecoder[Page] =
        value =>
          Either
            .catchOnly[NumberFormatException](value.value.toInt)
            .flatMap(Page.from)
            .leftMap(_ => new IllegalArgumentException(page.errorMessage(value.value)))
            .leftMap(_.getMessage)
            .leftMap(ParseFailure(_, ""))
            .toValidatedNel

      object page extends OptionalValidatingQueryParamDecoderMatcher[Page]("page") {
        val parameterName: String = "page"
        def errorMessage(value: String): String = s"'$value' not a valid Page number"
      }

      private implicit val perPageParameterDecoder: QueryParamDecoder[PerPage] =
        value =>
          Either
            .catchOnly[NumberFormatException](value.value.toInt)
            .flatMap(PerPage.from)
            .leftMap(_ => new IllegalArgumentException(perPage.errorMessage(value.value)))
            .leftMap(_.getMessage)
            .leftMap(ParseFailure(_, ""))
            .toValidatedNel

      object perPage extends OptionalValidatingQueryParamDecoderMatcher[PerPage]("per_page") {
        val parameterName: String = "per_page"
        def errorMessage(value: String): String = s"'$value' not a valid PerPage number"
      }
    }
  }
}
