package ch.datascience.tinytypes

trait TinyType[T] extends ConstraintCheck with TypeName {
  def value: T

  override lazy val toString: String = value.toString
}

trait ConstraintCheck {
  protected[this] def verify( requirement: Boolean, message: => String ): Unit =
    if ( !requirement ) throw new IllegalArgumentException( message )
}

trait TypeName {
  protected[this] lazy val typeName: String = getClass.getSimpleName
}

trait StringValue extends TinyType[String]
