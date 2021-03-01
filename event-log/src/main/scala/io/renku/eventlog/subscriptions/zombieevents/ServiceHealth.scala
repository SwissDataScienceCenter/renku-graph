package io.renku.eventlog.subscriptions.zombieevents

import ch.datascience.microservices.MicroserviceBaseUrl

private trait ServiceHealth[Interpretation[_]] {
  def ping(microserviceBaseUrl: MicroserviceBaseUrl): Interpretation[Boolean]
}
