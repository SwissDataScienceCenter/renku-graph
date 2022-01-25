package io.renku.knowledgegraph.entities

import io.renku.graph.model.users
import io.renku.tinytypes.constraints.NonBlank
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

object Endpoint {

  final case class Filters(maybeQuery:      Option[Endpoint.Filters.Query] = None,
                           maybeEntityType: Option[Endpoint.Filters.EntityType] = None,
                           maybeCreator:    Option[users.Name] = None
  )

  object Filters {

    final class Query private (val value: String) extends AnyVal with StringTinyType
    object Query                                  extends TinyTypeFactory[Query](new Query(_)) with NonBlank

    sealed trait EntityType extends StringTinyType with Product with Serializable
    object EntityType extends TinyTypeFactory[EntityType](EntityTypeApply) {
      final case object Project extends EntityType { override val value: String = "project" }
      final case object Dataset extends EntityType { override val value: String = "dataset" }
      val all: List[EntityType] = Project :: Dataset :: Nil
    }
    private object EntityTypeApply extends (String => EntityType) {
      override def apply(value: String): EntityType = EntityType.all.find(_.value == value).getOrElse {
        throw new IllegalArgumentException(s"'$value' unknown EntityType")
      }
    }
  }
}
