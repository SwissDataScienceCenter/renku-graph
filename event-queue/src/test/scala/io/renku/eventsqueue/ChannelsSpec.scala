package io.renku.eventsqueue

import cats.data.Kleisli
import cats.effect.{IO, Spawn, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.implicits._

import scala.concurrent.duration._
import scala.util.Random

class ChannelsSpec
  extends AnyWordSpec
    with IOSpec
    with InMemoryProjectsTokensDbSpec
    with should.Matchers
    with MockFactory {

  "use pg async notification mechanism for pub/sub purposes" in {

    type Ch = Channel[IO, String, String]

    def notify(ch: Ch) = Spawn[IO].start {
      Stream
        .iterate(1)(_ + 1)
        .evalMap { i =>
          println(s"sending $i")
          Temporal[IO].delayBy(ch.notify(i.toString), Random.nextInt(1000) millis)
        }
        .compile
        .drain
    }

    def listen(ch: Ch, listenerId: String) =
      ch.listen(20)
        .map(n => println(s"$listenerId received: ${n.value}; pid: ${n.pid} from ${n.channel} channel"))
        .compile
        .drain

    execute {
      Kleisli[IO, Session[IO], Ch] { session =>
        session.channel(id"test_channel").pure[IO]
      }.flatMapF { ch =>
        (notify(ch), listen(ch, "listener1"), listen(ch, "listener2")).parTupled.void
      }
    }
  }
}