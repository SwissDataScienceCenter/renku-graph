package ch.datascience.webhookservice.queue

import ch.datascience.tinytypes.StringValue
import ch.datascience.tinytypes.constraints.NonBlank

case class TriplesFile(value: String) extends StringValue with NonBlank
