package io.renku.testtools

import cats.Applicative
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.client.RestClient.ResponseMappingF
import org.http4s.{Method, Uri}
import org.scalacheck.Gen
import org.scalamock.matchers.ArgCapture.CaptureOne
import org.scalamock.scalatest.MockFactory
import io.renku.generators.Generators.Implicits._

trait GitLabClientTools[F[_]] {
  self: MockFactory =>

  def captureMapping[FinderType, ResultType](finder: FinderType, gitLabClient: GitLabClient[F])(
      findingMethod:                                 FinderType => ResultType,
      resultGenerator:                               Gen[ResultType]
  )(implicit applicative:                            Applicative[F]): ResponseMappingF[F, ResultType] = {
    val responseMapping = CaptureOne[ResponseMappingF[F, ResultType]]()

    (gitLabClient
      .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[F, ResultType])(
        _: Option[AccessToken]
      ))
      .expects(*, *, *, capture(responseMapping), *)
      .returning(resultGenerator.generateOne.pure[F])

    findingMethod(finder)

    responseMapping.value
  }
}
