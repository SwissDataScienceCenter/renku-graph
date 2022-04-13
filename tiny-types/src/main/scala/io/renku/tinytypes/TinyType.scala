/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.tinytypes

import cats.syntax.all._
import cats.{MonadThrow, Order, Show}
import io.circe.Json
import io.renku.tinytypes.constraints.PathSegment

import java.time.{Duration, Instant, LocalDate}

trait TinyType extends Any {

  type V

  def value: V

  override def toString: String = value.toString
}

trait StringTinyType       extends Any with TinyType { type V = String }
trait RelativePathTinyType extends Any with TinyType { type V = String }
trait UrlTinyType          extends Any with TinyType { type V = String }
trait IntTinyType          extends Any with TinyType { type V = Int }
trait LongTinyType         extends Any with TinyType { type V = Long }
trait FloatTinyType        extends Any with TinyType { type V = Float }
trait BigDecimalTinyType   extends Any with TinyType { type V = BigDecimal }
trait JsonTinyType         extends Any with TinyType { type V = Json }
trait InstantTinyType      extends Any with TinyType { type V = Instant }
trait DurationTinyType     extends Any with TinyType { type V = Duration }
trait LocalDateTinyType    extends Any with TinyType { type V = LocalDate }
trait BooleanTinyType      extends Any with TinyType { type V = Boolean }
trait ByteArrayTinyType    extends Any with TinyType { type V = Array[Byte] }

object StringTinyType {
  implicit val stringTinyTypeConverter: StringTinyType => List[PathSegment] =
    tinyType => List(PathSegment(tinyType.value))
}

object RelativePathTinyType {
  implicit val relativePathTinyTypeConverter: RelativePathTinyType => List[PathSegment] =
    tinyType => tinyType.value.split("\\/").toList.map(PathSegment.apply)
}

object IntTinyType {
  implicit val IntTinyTypeConverter: IntTinyType => List[PathSegment] =
    tinyType => List(PathSegment(tinyType.toString))
}

trait Sensitive extends Any {
  self: TinyType =>
  override def toString: String = "<sensitive>"
}

abstract class TinyTypeFactory[TT <: TinyType](instantiate: TT#V => TT)
    extends (TT#V => TT)
    with From[TT]
    with Constraints[TT#V]
    with ValueTransformation[TT#V]
    with TinyTypeOrdering[TT]
    with TypeName {

  import scala.util.Try

  final def apply(value: TT#V): TT = from(value).fold(
    exception => throw exception,
    identity
  )

  final def unapply(tinyType: TT): Some[TT#V] = Some(tinyType.value)

  final def from(value: TT#V): Either[IllegalArgumentException, TT] = for {
    transformed <- transform(value) leftMap flattenErrors
    validated   <- validate(transformed)
  } yield validated

  private def validate(value: TT#V): Either[IllegalArgumentException, TT] = {
    val maybeErrors = validateConstraints(value)
    if (maybeErrors.isEmpty) Either.fromTry[TT](Try(instantiate(value))) leftMap flattenErrors
    else Left(new IllegalArgumentException(maybeErrors.mkString("; ")))
  }

  private lazy val flattenErrors: Throwable => IllegalArgumentException = {
    case exception: IllegalArgumentException => exception
    case exception => new IllegalArgumentException(exception)
  }

  implicit class TinyTypeConverters(tinyType: TT) {

    def as[F[_]: MonadThrow, OUT](implicit converter: TinyTypeConverter[TT, OUT]): F[OUT] =
      MonadThrow[F].fromEither(converter(tinyType))

    def toUnsafe[OUT](implicit convert: TT => Either[Exception, OUT]): OUT =
      convert(tinyType).fold(throw _, identity)

    def showAs[View](implicit renderer: Renderer[View, TT]): String = renderer.render(tinyType)
  }

  implicit lazy val show: Show[TT] = Show.show(_.toString)
}

trait TinyTypeConverter[TT <: TinyType, OUT] extends (TT => Either[Exception, OUT])

trait Renderer[View, -T] {
  def render(value: T): String
}

trait From[TT <: TinyType] {
  def from(value: TT#V): Either[IllegalArgumentException, TT]
}

trait TinyTypeOrdering[TT <: TinyType] {
  self: TinyTypeFactory[TT] =>

  implicit class TinyTypeOps(tinyType: TT)(implicit val ord: Ordering[TT#V]) {
    def compareTo(other: TT): Int =
      ord.compare(tinyType.value, other.value)
  }

  implicit def ordering(implicit valueOrdering: Ordering[TT#V]): Ordering[TT] =
    Ordering.by(_.value)

  implicit def order(implicit ordering: Ordering[TT]): Order[TT] = Order.fromOrdering
}

trait Constraints[V] extends TypeName {

  private val constraints: collection.mutable.ListBuffer[Constraint] = collection.mutable.ListBuffer.empty

  def addConstraint(check: V => Boolean, message: V => String): Unit =
    constraints += Constraint(check, message)

  private case class Constraint(check: V => Boolean, message: V => String)

  protected def validateConstraints(value: V): Seq[String] = constraints.foldLeft(Seq.empty[String]) {
    case (errors, constraint) =>
      if (!constraint.check(value)) errors :+ constraint.message(value)
      else errors
  }
}

trait ValueTransformation[T] extends TypeName {
  val transform: T => Either[Throwable, T] = value => Right(value)
}

trait TypeName {

  lazy val typeName: String = {
    val className = getClass.getName.replace("$", ".")
    if (className.endsWith(".")) className take className.length - 1
    else className
  }

  lazy val shortTypeName: String = {
    val lastDotIndex = typeName.lastIndexOf(".")
    if (lastDotIndex > -1) typeName.substring(lastDotIndex + 1)
    else typeName
  }
}
