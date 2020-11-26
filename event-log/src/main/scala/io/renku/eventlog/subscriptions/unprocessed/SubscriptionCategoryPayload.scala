package io.renku.eventlog.subscriptions.unprocessed

import io.renku.eventlog.subscriptions

case class SubscriptionCategoryPayload(override val subscriberUrl: subscriptions.SubscriberUrl)
    extends subscriptions.SubscriptionCategoryPayload
