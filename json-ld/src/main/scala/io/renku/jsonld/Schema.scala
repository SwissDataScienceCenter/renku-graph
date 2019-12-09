package io.renku.jsonld

abstract class Schema(value: String, separator: String = "/") extends Product with Serializable {
  def /(name: String): Property = Property(s"$value$separator$name")
}

final case class Property(url: String) {
  override lazy val toString: String = url
}

object Schema {

  def from(baseUrl: String): Schema = StandardSchema(baseUrl)

  private[jsonld] final case class StandardSchema(value: String) extends Schema(value)
}
