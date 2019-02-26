package ch.datascience.webhookservice

import cats.MonadError
import cats.effect._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext

class MicroserviceRunnerSpec extends WordSpec with MockFactory {

  "run" should {

    "return Success Exit Code if http server start-up were successful" in new TestCase {

      (httpServer.run _)
        .expects()
        .returning(context.pure(ExitCode.Success))

      runner.run(Nil).unsafeRunSync() shouldBe ExitCode.Success
    }

    "fail if starting http server fails" in new TestCase {

      val exception = Generators.exceptions.generateOne
      (httpServer.run _)
        .expects()
        .returning(context.raiseError(exception))

      intercept[Exception] {
        runner.run(Nil).unsafeRunSync()
      } shouldBe exception
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val httpServer = mock[IOHttpServer]
    val runner     = new MicroserviceRunner(httpServer)
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private class IOHttpServer() extends HttpServer[IO]()
}
