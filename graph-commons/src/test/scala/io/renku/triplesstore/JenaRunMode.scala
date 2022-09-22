package io.renku.triplesstore

import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive

sealed trait JenaRunMode

object JenaRunMode {

  /** A docker container is started creating a port mapping using the given port. */
  final case class FixedPortContainer(port: Int Refined Positive) extends JenaRunMode

  /** A docker container is started using a random port mapping. */
  case object GenericContainer extends JenaRunMode

  /** No container is started, it is assumed that Jena is running and accepts connections at the given port */
  final case class Local(port: Int Refined Positive) extends JenaRunMode
}
