package ch.datascience.triplesgenerator.events

private sealed trait EventSchedulingResult extends Product with Serializable

private object EventSchedulingResult {
  case object Accepted             extends EventSchedulingResult
  case object Busy                 extends EventSchedulingResult
  case object UnsupportedEventType extends EventSchedulingResult
  case object BadRequest           extends EventSchedulingResult
  case object SchedulingError      extends EventSchedulingResult
}
