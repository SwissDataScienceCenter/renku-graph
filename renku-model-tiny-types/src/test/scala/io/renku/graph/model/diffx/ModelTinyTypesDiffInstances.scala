package io.renku.graph.model.diffx

import com.softwaremill.diffx.Diff
import io.renku.graph.model.{commandParameters, datasets, projects}
import io.renku.graph.model.images.{Image, ImageUri}

trait ModelTinyTypesDiffInstances extends TinyTypeDiffInstances {

  implicit val imageUriDiff: Diff[ImageUri] =
    Diff.diffForString.contramap(_.value)

  implicit val datasetsDateDiff: Diff[datasets.Date] =
    Diff.derived[datasets.Date]

  implicit val imageDiff: Diff[Image] =
    Diff.derived[Image]

  implicit val projectPathDiff: Diff[projects.Path] =
    Diff.diffForString.contramap(_.value)

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

  implicit val visibilityDiff: Diff[projects.Visibility] =
    Diff.derived[projects.Visibility]

}

object ModelTinyTypesDiffInstances extends ModelTinyTypesDiffInstances
