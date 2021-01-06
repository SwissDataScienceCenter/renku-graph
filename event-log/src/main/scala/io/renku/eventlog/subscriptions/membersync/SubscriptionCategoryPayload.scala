package io.renku.eventlog.subscriptions.membersync

import io.renku.eventlog.subscriptions

private case class SubscriptionCategoryPayload(override val subscriberUrl: subscriptions.SubscriberUrl)
    extends subscriptions.SubscriptionCategoryPayload
