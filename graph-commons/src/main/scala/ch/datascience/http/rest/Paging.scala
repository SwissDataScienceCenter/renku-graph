package ch.datascience.http.rest

object Paging {

  import ch.datascience.http.rest.Paging.PagingRequest._

  case class PagingRequest(page: Page, perPage: PerPage)

  object PagingRequest {
    import cats.implicits._
    import ch.datascience.tinytypes.constraints.PositiveInt
    import ch.datascience.tinytypes.{IntTinyType, TinyTypeFactory}
    import org.http4s.dsl.impl.OptionalValidatingQueryParamDecoderMatcher
    import org.http4s.{ParseFailure, QueryParamDecoder}

    def apply(maybePage: Option[Page], maybePerPage: Option[PerPage]): PagingRequest =
      PagingRequest(maybePage getOrElse Page.first, maybePerPage getOrElse PerPage.default)

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
            .leftMap(_ => new IllegalArgumentException(s"'${value.value}' not a valid Page number"))
            .leftMap(_.getMessage)
            .leftMap(ParseFailure(_, ""))
            .toValidatedNel

      object page extends OptionalValidatingQueryParamDecoderMatcher[Page]("page") {
        val parameterName: String = "page"
      }

      private implicit val perPageParameterDecoder: QueryParamDecoder[PerPage] =
        value =>
          Either
            .catchOnly[NumberFormatException](value.value.toInt)
            .flatMap(PerPage.from)
            .leftMap(_ => new IllegalArgumentException(s"'${value.value}' not a valid PerPage number"))
            .leftMap(_.getMessage)
            .leftMap(ParseFailure(_, ""))
            .toValidatedNel

      object perPage extends OptionalValidatingQueryParamDecoderMatcher[PerPage]("per_page") {
        val parameterName: String = "per_page"
      }
    }
  }
}
