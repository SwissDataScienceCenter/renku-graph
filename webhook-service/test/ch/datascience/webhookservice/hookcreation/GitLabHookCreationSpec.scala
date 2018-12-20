/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.datascience.webhookservice.hookcreation

import cats.data.EitherT
import cats.effect.IO
import cats.kernel.Monoid
import fs2.Stream
import org.http4s.{ Method, Request, Status, Uri }
import org.http4s.Status.Ok
import org.http4s.client.blaze.Http1Client
import org.scalatest.WordSpec

class GitLabHookCreationSpec extends WordSpec {

  //  val request = Request[IO](method = Method.GET, uri = Uri.uri("http://localhost:9001/pingd"))
  //  val result = for {
  //    httpClient <- Http1Client.stream[IO]()
  //    re <- httpClient.streaming(request) { response =>
  //      response.status match {
  //        case Ok => Stream.apply(IO(Ok))
  //      }
  //    }
  //  } yield re
  //
  //  import cats.implicits._, cats.effect.IO
  //
  //  implicit val monoid: Monoid[Status] = new Monoid[Status] {
  //    override def empty: Status = throw new RuntimeException("")
  //
  //    override def combine(x: Status, y: Status): Status = x
  //  }
  //
  //  val attempt = result.compile.toVector.map(_.head).attempt
  //  println(attempt)
  //  val status = EitherT(attempt.unsafeToFuture())
  //  println(status.value.futureValue)

}
