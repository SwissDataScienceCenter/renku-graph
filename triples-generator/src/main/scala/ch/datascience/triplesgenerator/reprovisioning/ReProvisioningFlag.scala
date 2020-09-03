package ch.datascience.triplesgenerator.reprovisioning

import scala.language.higherKinds

trait ReProvisioningFlag[Interpretation[_]] {
  def currentlyReProvisioning: Interpretation[Boolean]
}

object ReProvisioningFlag {
  def apply[Interpretation[_]](): Interpretation[ReProvisioningFlag[Interpretation]] = ???
}
