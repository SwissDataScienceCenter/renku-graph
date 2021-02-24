package ch.datascience.microservices

import cats.MonadError
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive

trait MicroserviceUrlFinder[Interpretation[_]] {
  def findSubscriberUrl(): Interpretation[SubscriberUrl]
}

class MicroserviceUrlFinderImpl[Interpretation[_]: MonadError[*[_], Throwable]](
    microservicePort: Int Refined Positive
) extends MicroserviceUrlFinder[Interpretation] {

  import java.net.NetworkInterface
  import cats.syntax.all._
  import scala.jdk.CollectionConverters._

  override def findSubscriberUrl(): Interpretation[SubscriberUrl] =
    findAddress flatMap {
      case Some(address) =>
        SubscriberUrl(s"http://${address.getHostAddress}:$microservicePort/events").pure[Interpretation]
      case None =>
        new Exception("Cannot find service IP").raiseError[Interpretation, SubscriberUrl]
    }

  private def findAddress = MonadError[Interpretation, Throwable].catchNonFatal {
    val ipAddresses = NetworkInterface.getNetworkInterfaces.asScala.toSeq.flatMap(p => p.getInetAddresses.asScala.toSeq)
    ipAddresses
      .find { address =>
        address.getHostAddress.contains(".") &&
        !address.isLoopbackAddress &&
        !address.isAnyLocalAddress &&
        !address.isLinkLocalAddress
      }
  }
}

object IOMicroserviceUrlFinder {
  import cats.effect.IO

  def apply(microservicePort: Int Refined Positive): IO[MicroserviceUrlFinder[IO]] = IO {
    new MicroserviceUrlFinderImpl[IO](microservicePort)
  }
}
