/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.diffx

import com.softwaremill.diffx.Diff
import io.renku.graph.model.images.{Image, ImageUri}
import io.renku.graph.model.{commandParameters, datasets, projects}

trait ModelTinyTypesDiffInstances extends TinyTypeDiffInstances {

  implicit val absoluteImageUriDiff: Diff[ImageUri.Absolute] = Diff.diffForString.contramap(_.value)
  implicit val relativeImageUriDiff: Diff[ImageUri.Relative] = Diff.diffForString.contramap(_.value)
  implicit val imageUriDiff:         Diff[ImageUri]          = Diff.diffForString.contramap(_.value)

  implicit val datasetsCreatedOrPublishedDiff: Diff[datasets.CreatedOrPublished] =
    Diff.derived[datasets.CreatedOrPublished]
  implicit val datasetsDateModified: Diff[datasets.DateModified] =
    Diff.derived[datasets.DateModified]

  implicit val datasetsSameAsDiff: Diff[datasets.SameAs] = Diff.diffForString.contramap {
    case sa: datasets.InternalSameAs => sa.value
    case sa: datasets.ExternalSameAs => sa.value
  }

  implicit val imageDiff: Diff[Image] = Diff.derived[Image]

  implicit val projectSlugDiff: Diff[projects.Slug] = Diff.diffForString.contramap(_.value)

  implicit lazy val outputDefaultValueDiff: Diff[commandParameters.OutputDefaultValue] =
    Diff.diffForString.contramap(_.value.value)

  implicit val inputDefaultValueDiff: Diff[commandParameters.InputDefaultValue] =
    Diff.diffForString.contramap(_.value.value)

  implicit val ioStreamInDiff: Diff[commandParameters.IOStream.In] = Diff.derived[commandParameters.IOStream.In]

  implicit val ioStreamOutDiff: Diff[commandParameters.IOStream.Out] = Diff.derived[commandParameters.IOStream.Out]

  implicit val ioStreamStdInDiff: Diff[commandParameters.IOStream.StdIn] =
    Diff.derived[commandParameters.IOStream.StdIn]
  implicit val ioStreamStdOutDiff: Diff[commandParameters.IOStream.StdOut] =
    Diff.derived[commandParameters.IOStream.StdOut]
  implicit val ioStreamErrOutDiff: Diff[commandParameters.IOStream.StdErr] =
    Diff.derived[commandParameters.IOStream.StdErr]

  implicit val visibilityDiff: Diff[projects.Visibility] = Diff.derived[projects.Visibility]

  implicit val roleDiff: Diff[projects.Role] =
    Diff.derived[projects.Role]
}

object ModelTinyTypesDiffInstances extends ModelTinyTypesDiffInstances
