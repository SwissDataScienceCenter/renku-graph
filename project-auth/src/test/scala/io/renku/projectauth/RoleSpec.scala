package io.renku.projectauth

import cats.data.NonEmptyList
import io.renku.projectauth.Role._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class RoleSpec extends AnyFlatSpec with should.Matchers {

  it should "order roles correctly" in {
    val roles = Role.all.sorted
    roles                            shouldBe NonEmptyList.of(Reader, Maintainer, Owner)
    Role.all.toList.sorted           shouldBe roles.toList
    Role.all.toList.sortBy(identity) shouldBe roles.toList
  }
}
