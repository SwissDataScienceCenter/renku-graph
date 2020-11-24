package io.renku.eventlog.subscriptions

import ch.datascience.generators.Generators.httpUrls
import org.scalacheck.Gen

object Generators {
  implicit val subscriberUrls: Gen[SubscriberUrl] = httpUrls() map SubscriberUrl.apply
}
