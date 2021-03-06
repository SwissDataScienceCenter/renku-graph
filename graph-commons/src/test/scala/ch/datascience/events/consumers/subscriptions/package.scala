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

package ch.datascience.events.consumers

import ch.datascience.generators.Generators.httpUrls
import ch.datascience.microservices.MicroserviceIdentifier
import org.scalacheck.Gen

package object subscriptions {
  implicit val subscriberIds:  Gen[SubscriberId]  = Gen.uuid map (_ => SubscriberId(MicroserviceIdentifier.generate))
  implicit val subscriberUrls: Gen[SubscriberUrl] = httpUrls() map SubscriberUrl.apply
}
