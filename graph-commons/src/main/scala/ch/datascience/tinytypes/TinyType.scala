/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.tinytypes

trait TinyType[T] extends Any {

  def value: T

  override def toString: String = value.toString
}

abstract class TinyTypeFactory[V, TT <: TinyType[V]]( instantiate: V => TT )
  extends Constraints[V]
  with TypeName {

  final def apply( value: V ): TT = {
    verify( value )
    instantiate( value )
  }

  final def unapply( sha: TT ): Option[V] = Some( sha.value )

  final def from( value: V ): Either[String, TT] = {
    val maybeErrors = validateConstraints( value )
    if ( maybeErrors.isEmpty ) Right( instantiate( value ) )
    else Left( maybeErrors.mkString( "; " ) )
  }

  private def verify( value: V ): Unit = {
    val maybeErrors = validateConstraints( value )
    if ( maybeErrors.nonEmpty ) throw new IllegalArgumentException( maybeErrors.mkString( "; " ) )
  }
}

trait Constraints[V] extends TypeName {

  private val constraints: collection.mutable.ListBuffer[Constraint] = collection.mutable.ListBuffer.empty

  def addConstraint( check: V => Boolean, message: V => String ): Unit =
    constraints += Constraint( check, message )

  private case class Constraint( check: V => Boolean, message: V => String )

  protected def validateConstraints( value: V ): Seq[String] = constraints.foldLeft( Seq.empty[String] ) {
    case ( errors, constraint ) =>
      if ( !constraint.check( value ) ) errors :+ constraint.message( value )
      else errors
  }
}

trait TypeName {
  protected[this] lazy val typeName: String = getClass.getSimpleName.replace( "$", "" )
}
