package io.renku.http.client

import cats.effect.IO
import com.comcast.ip4s._
import eu.timepit.refined.auto._
import io.renku.control.Throttler
import io.renku.http.server.SimpleServer
import io.renku.testtools.IOSpec
import org.http4s.implicits._
import org.http4s.{Method, Status, Uri}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration._

class ClientTimeoutTest extends AnyFlatSpec with should.Matchers with IOSpec {

  it should "be configurable for request timeouts" ignore {
    val client = new ClientTimeoutTest.MyClient(Some(90.seconds))

    SimpleServer
      .server(port"8088")
      .use { _ =>
        client.get(uri"http://localhost:8088").map(_ shouldBe Status.Ok)
      }
      .unsafeRunSync()
  }
}

object ClientTimeoutTest {
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger

  final class MyClient(timeout: Option[Duration])
      extends RestClient[IO, Nothing](Throttler.noThrottling, None, 5.seconds, 5, timeout, timeout) {
    def get(uri: Uri): IO[Status] =
      send(request(Method.GET, uri)) { case r => IO.pure(r._1) }
  }
}
