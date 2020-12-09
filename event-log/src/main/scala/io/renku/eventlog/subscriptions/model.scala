/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions

import ch.datascience.graph.model.projects
import ch.datascience.tinytypes.constraints.Url
import ch.datascience.tinytypes.json.TinyTypeDecoders.stringDecoder
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import io.circe.Decoder

private final case class ProjectIds(id: projects.Id, path: projects.Path)

private final class SubscriberUrl private (val value: String) extends AnyVal with StringTinyType
private object SubscriberUrl extends TinyTypeFactory[SubscriberUrl](new SubscriberUrl(_)) with Url {
  implicit val subscriberUrlDecoder: Decoder[SubscriberUrl] = stringDecoder(SubscriberUrl)
}

private trait SubscriptionCategoryPayload {
  def subscriberUrl: SubscriberUrl
}
