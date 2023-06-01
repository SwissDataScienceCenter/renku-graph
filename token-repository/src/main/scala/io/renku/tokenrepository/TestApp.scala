package io.renku.tokenrepository

import cats.effect.{ExitCode, IO, IOApp}
import io.circe.{Decoder, Encoder}
import fs2.Stream
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.circe.CirceEntityCodec._
import org.http4s.client.Client
import org.http4s.syntax.all._
import org.http4s.client.dsl.io._
import org.http4s.dsl.io._

object TestApp extends IOApp {
  final case class Payload(personalAccessToken: String)
  implicit val jsonEncoder: Encoder[Payload] =
    io.circe.generic.semiauto.deriveEncoder
  implicit val jsonDecoder: Decoder[Payload] =
    io.circe.generic.semiauto.deriveDecoder

  val standardClient = EmberClientBuilder
    .default[IO]
    .withHttp2
    .build

  val projectId   = "42"
  val tokenValue  = Payload("dG9rZW4xMjM=") // token123
  val baseUri     = uri"http://localhost:9003"
  val numRequests = 50
  val concurrency = 6

  val addReq = POST(tokenValue, baseUri / "projects" / projectId / "tokens")

  def runRequest(c: Client[IO]) =
    for {
      inserted <- c.successful(addReq)
      _        <- IO.println(s"Inserting token success: $inserted")
      _        <- if (!inserted) IO.raiseError(new Exception("oh no!")) else IO.unit
    } yield ()

  def runRequestR =
    standardClient.use(runRequest)

  def reproduce =
    Stream
      .range(0, numRequests)
      .covary[IO]
      .parEvalMapUnordered(concurrency)(_ => runRequestR)

  def workingFine =
    Stream
      .resource(standardClient)
      .flatMap(c => Stream.range(0, numRequests).covary[IO].parEvalMapUnordered(concurrency)(_ => runRequest(c)))

  override def run(args: List[String]): IO[ExitCode] =
    reproduce.compile.drain
      .as(ExitCode.Success)

}
