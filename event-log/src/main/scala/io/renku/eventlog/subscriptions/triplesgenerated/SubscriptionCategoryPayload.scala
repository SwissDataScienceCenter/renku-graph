/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.eventlog.subscriptions.triplesgenerated

import ch.datascience.events.consumers.subscriptions.{SubscriberId, SubscriberUrl}
import io.renku.eventlog.subscriptions
import io.renku.eventlog.subscriptions.Capacity

private case class SubscriptionCategoryPayload(subscriberUrl: SubscriberUrl,
                                               subscriberId:  SubscriberId,
                                               maybeCapacity: Option[Capacity]
) extends subscriptions.SubscriptionInfo
