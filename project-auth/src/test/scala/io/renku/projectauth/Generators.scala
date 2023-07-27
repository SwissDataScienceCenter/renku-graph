package io.renku.projectauth

import cats.effect.IO
import cats.arrow.FunctionK
import cats.~>
import fs2.Stream
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators
import org.scalacheck.Gen

object Generators {
  val genToIO: Gen ~> IO =
    FunctionK.lift[Gen, IO](gen => IO(gen.generateOne))

  val roleGen: Gen[Role] =
    Gen.oneOf(Role.all.toList)

  val memberGen: Gen[ProjectMember] = for {
    name <- RenkuTinyTypeGenerators.personNames
    role <- roleGen
    id   <- RenkuTinyTypeGenerators.personGitLabIds
  } yield ProjectMember(name, id, role)

  val projectAuthDataGen: Gen[ProjectAuthData] = for {
    id         <- RenkuTinyTypeGenerators.projectIds
    path       <- RenkuTinyTypeGenerators.projectPaths
    members    <- Gen.choose(0, 15).flatMap(n => Gen.listOfN(n, memberGen))
    visibility <- RenkuTinyTypeGenerators.projectVisibilities
  } yield ProjectAuthData(id, path, members.toSet, visibility)

  def projectAuthDataStream: Stream[Gen, ProjectAuthData] =
    Stream.eval(projectAuthDataGen) ++ projectAuthDataStream
}
