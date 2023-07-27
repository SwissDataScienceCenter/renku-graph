package io.renku.triplesstore.client.http

import org.http4s.client.ConnectionFailure
import org.http4s.ember.core.EmberException

import java.io.IOException
import java.net.{ConnectException, SocketException, UnknownHostException}
import java.nio.channels.ClosedChannelException

object ConnectionError {

  def exists(ex: Throwable): Boolean =
    unapply(ex).isDefined

  def unapply(ex: Throwable): Option[Throwable] =
    ex match {
      case _: ConnectionFailure | _: ConnectException | _: SocketException | _: UnknownHostException =>
        Some(ex)
      case _: IOException
          if ex.getMessage.toLowerCase
            .contains("connection reset") || ex.getMessage.toLowerCase.contains("broken pipe") =>
        Some(ex)
      case _: EmberException.ReachedEndOfStream => Some(ex)
      case _: ClosedChannelException            => Some(ex)
      case _ => None
    }
}
