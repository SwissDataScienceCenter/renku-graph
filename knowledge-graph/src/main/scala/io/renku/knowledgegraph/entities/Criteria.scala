package io.renku.knowledgegraph.entities

import Criteria._
import io.circe.Decoder
import io.renku.graph.model.{persons, projects}
import io.renku.http.rest.SortBy.Direction
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.server.security.model.AuthUser
import io.renku.tinytypes.constraints.{LocalDateNotInTheFuture, NonBlank}
import io.renku.tinytypes.json.TinyTypeDecoders
import io.renku.tinytypes.{LocalDateTinyType, StringTinyType, TinyTypeFactory}

import java.time.LocalDate

final case class Criteria(filters:   Filters = Filters(),
                          sorting:   Sorting.By = Sorting.default,
                          paging:    PagingRequest = PagingRequest.default,
                          maybeUser: Option[AuthUser] = None
)

object Criteria {

  final case class Filters(maybeQuery:   Option[Filters.Query] = None,
                           entityTypes:  Set[Filters.EntityType] = Set.empty,
                           creators:     Set[persons.Name] = Set.empty,
                           visibilities: Set[projects.Visibility] = Set.empty,
                           namespaces:   Set[projects.Namespace] = Set.empty,
                           maybeSince:   Option[Filters.Since] = None,
                           maybeUntil:   Option[Filters.Until] = None
  )

  object Filters {

    final class Query private (val value: String) extends AnyVal with StringTinyType
    object Query                                  extends TinyTypeFactory[Query](new Query(_)) with NonBlank[Query]

    sealed trait EntityType extends StringTinyType with Product with Serializable
    object EntityType extends TinyTypeFactory[EntityType](EntityTypeApply) {

      final case object Project  extends EntityType { override val value: String = "project" }
      final case object Dataset  extends EntityType { override val value: String = "dataset" }
      final case object Workflow extends EntityType { override val value: String = "workflow" }
      final case object Person   extends EntityType { override val value: String = "person" }

      val all: List[EntityType] = Project :: Dataset :: Workflow :: Person :: Nil

      implicit val decoder: Decoder[EntityType] = TinyTypeDecoders.stringDecoder(EntityType)
    }

    private object EntityTypeApply extends (String => EntityType) {
      override def apply(value: String): EntityType = EntityType.all.find(_.value == value).getOrElse {
        throw new IllegalArgumentException(s"'$value' unknown EntityType")
      }
    }

    final class Since private (val value: LocalDate) extends AnyVal with LocalDateTinyType
    object Since extends TinyTypeFactory[Since](new Since(_)) with LocalDateNotInTheFuture[Since]

    final class Until private (val value: LocalDate) extends AnyVal with LocalDateTinyType
    object Until extends TinyTypeFactory[Until](new Until(_)) with LocalDateNotInTheFuture[Until]
  }

  object Sorting extends io.renku.http.rest.SortBy {

    type PropertyType = SortProperty

    sealed trait SortProperty extends Property

    final case object ByName          extends Property("name") with SortProperty
    final case object ByMatchingScore extends Property("matchingScore") with SortProperty
    final case object ByDate          extends Property("date") with SortProperty

    lazy val default: Sorting.By = Sorting.By(ByName, Direction.Asc)

    override lazy val properties: Set[SortProperty] = Set(ByName, ByMatchingScore, ByDate)
  }
}
