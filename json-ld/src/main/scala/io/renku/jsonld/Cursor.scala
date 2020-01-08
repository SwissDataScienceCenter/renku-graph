package io.renku.jsonld

import io.renku.jsonld.JsonLD.JsonLDEntity

abstract class Cursor {

  import Cursor._

  val jsonLD: JsonLD

  def as[T](implicit decoder: JsonLDDecoder[T]): JsonLDDecoder.Result[T] = decoder(this)

  def downType(searchedType: EntityType): Cursor = jsonLD match {
    case JsonLDEntity(_, types, _) if types.list.exists(_ == searchedType) => this
    case _                                                                 => Empty
  }

  def downField(name: Property): Cursor = jsonLD match {
    case JsonLDEntity(_, _, props) =>
      props
        .find(_._1 == name)
        .fold(Empty: Cursor) { case (name, value) => new PropertyCursor(this, name, value) }
    case _ => Empty
  }
}

object Cursor {
  def from(jsonLD: JsonLD): Cursor = new TopCursor(jsonLD)

  private[jsonld] object Empty extends Cursor { override val jsonLD = JsonLD.Null }

  private[jsonld] class TopCursor(override val jsonLD: JsonLD) extends Cursor

  private[jsonld] class PropertyCursor(parent: Cursor, property: Property, override val jsonLD: JsonLD) extends Cursor
}
