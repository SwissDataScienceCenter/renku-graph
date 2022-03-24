package io.renku.eventlog.subscriptions.tsmigration

import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.http.server.version.ServiceVersion

final case class MigrationRequestEvent(subscriberUrl: SubscriberUrl, subscriberVersion: ServiceVersion)
