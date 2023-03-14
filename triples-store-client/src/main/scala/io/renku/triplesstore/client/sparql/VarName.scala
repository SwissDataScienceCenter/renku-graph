package io.renku.triplesstore.client.sparql

final class VarName private (val name: String) extends AnyVal

object VarName {
  def apply(name: String): VarName =
    if (name.startsWith("?")) new VarName(name)
    else new VarName(s"?$name")
}
