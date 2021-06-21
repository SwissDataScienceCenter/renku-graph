package ch.datascience.commiteventservice.events.categories.common

sealed trait UpdateResult extends Product with Serializable

object UpdateResult {
  final case object Skipped extends UpdateResult
  final case object Created extends UpdateResult
  final case object Existed extends UpdateResult
  final case object Deleted extends UpdateResult
  case class Failed(message: String, exception: Throwable) extends UpdateResult
}
