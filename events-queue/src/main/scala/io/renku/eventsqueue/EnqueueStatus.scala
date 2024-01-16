/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventsqueue

import cats.Show

private sealed trait EnqueueStatus {
  val value:   String
  val dbValue: Short
  override lazy val toString: String        = value
  lazy val widen:             EnqueueStatus = this
}

private object EnqueueStatus {
  case object New extends EnqueueStatus {
    override val value:   String = "NEW"
    override val dbValue: Short  = 0
  }
  case object Processing extends EnqueueStatus {
    override val value:   String = "PROCESSING"
    override val dbValue: Short  = 1
  }

  lazy val all: Set[EnqueueStatus] = Set(New, Processing)

  def apply(value: String): EnqueueStatus =
    EnqueueStatus.all
      .find(_.value == value)
      .getOrElse(throw new IllegalArgumentException(s"'$value' unknown EnqueueStatus"))

  implicit def show[S <: EnqueueStatus]: Show[S] = Show.fromToString
}
