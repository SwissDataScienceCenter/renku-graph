package ch.datascience.http
import cats.effect.IO
import org.http4s.{EntityDecoder, Response, Status}

object EndpointTester {

  implicit class IOResponseOps(response: IO[Response[IO]]) {

    private val runResponse: Response[IO] = response.unsafeRunSync()

    lazy val status: Status = runResponse.status

    def body[T](implicit decoder: EntityDecoder[IO, T]): T = runResponse.as[T].unsafeRunSync
  }
}
