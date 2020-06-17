package ch.datascience.rdfstore.entities

trait PartialEntityConverter[S] {
  def convert[T <: S]: T => Either[Exception, PartialEntity]
}
