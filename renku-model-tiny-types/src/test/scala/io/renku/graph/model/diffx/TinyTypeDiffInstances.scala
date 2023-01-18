package io.renku.graph.model.diffx

import cats.data.NonEmptyList
import com.softwaremill.diffx.Diff
import io.renku.tinytypes._

trait TinyTypeDiffInstances {
  implicit def nonEmptyListDiff[A: Diff]: Diff[NonEmptyList[A]] =
    Diff.diffForSeq[List, A].contramap(_.toList)

  implicit def stringTinyTypeDiff[A <: StringTinyType]: Diff[A] =
    Diff.diffForString.contramap(_.value)

  implicit def urlTinyTypeDiff[A <: UrlTinyType]: Diff[A] =
    Diff.diffForString.contramap(_.value)

  implicit def instantTinyTypeDiff[A <: InstantTinyType]: Diff[A] =
    Diff.diffForString.contramap(_.value.toString)

  implicit def localDateTinyTypeDiff[A <: LocalDateTinyType]: Diff[A] =
    Diff.diffForString.contramap(_.value.toString)

  implicit def durationTinyTypeDiff[A <: DurationTinyType]: Diff[A] =
    Diff.diffForString.contramap(_.value.toString)

  implicit def intTinyTypeDiff[A <: IntTinyType]: Diff[A] =
    Diff.diffForNumeric[Int].contramap(_.value)

  implicit def longTinyTypeDiff[A <: LongTinyType]: Diff[A] =
    Diff.diffForNumeric[Long].contramap(_.value)

  implicit def bigDecimalTinyTypeDiff[A <: BigDecimalTinyType]: Diff[A] =
    Diff.diffForNumeric[BigDecimal].contramap(_.value)

  implicit def floatTinyTypeDiff[A <: FloatTinyType]: Diff[A] =
    Diff.diffForNumeric[Float].contramap(_.value)

  implicit def boolTinyTypeDiff[A <: BooleanTinyType]: Diff[A] =
    Diff.diffForBoolean.contramap(_.value)

  implicit def relativePathTinyType[A <: RelativePathTinyType]: Diff[A] =
    Diff.diffForString.contramap(_.value)

}

object TinyTypeDiffInstances extends TinyTypeDiffInstances
