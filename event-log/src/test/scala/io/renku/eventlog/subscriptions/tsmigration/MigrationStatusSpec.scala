package io.renku.eventlog.subscriptions.tsmigration

import io.circe.Json
import io.renku.data.ErrorMessage
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.tinytypes.constraints.NonBlank
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class MigrationStatusSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {
  import MigrationStatus._

  "MigrationStatus" should {

    val scenarios = Table(
      "String Value" -> "Expected Status",
      MigrationStatus.all.toList.map {
        case New                   => "NEW"                     -> New
        case Sent                  => "SENT"                    -> Sent
        case Done                  => "DONE"                    -> Done
        case NonRecoverableFailure => "NON_RECOVERABLE_FAILURE" -> NonRecoverableFailure
        case RecoverableFailure    => "RECOVERABLE_FAILURE"     -> RecoverableFailure
      }: _*
    )

    forAll(scenarios) { (stringValue, expectedStatus) =>
      s"be instantiatable from '$stringValue'" in {
        MigrationStatus.from(stringValue) shouldBe Right(expectedStatus)
      }

      s"be deserializable from $stringValue" in {
        Json.fromString(stringValue).as[MigrationStatus] shouldBe Right(expectedStatus)
      }
    }

    "fail instantiation for unknown value" in {
      val unknown = nonEmptyStrings().generateOne

      val Left(exception) = MigrationStatus.from(unknown)

      exception.getMessage shouldBe s"'$unknown' unknown MigrationStatus"
    }

    "fail deserialization for unknown value" in {
      val unknown = nonEmptyStrings().generateOne

      val Left(exception) = Json.fromString(unknown).as[MigrationStatus]

      exception.getMessage shouldBe s"'$unknown' unknown MigrationStatus"
    }
  }
}

class ChangeDateSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {
  import java.time.temporal.ChronoUnit.SECONDS

  "instantiation" should {

    "succeed for timestamps not in the future" in {
      forAll(timestamps(max = Instant.now())) { value =>
        ChangeDate.from(value).map(_.value) shouldBe Right(value)
      }
    }

    "fail for timestamps from the future" in {
      forAll(timestamps(min = Instant.now().plus(1, SECONDS))) { value =>
        val Left(exception) = ChangeDate.from(value).map(_.value)
        exception            shouldBe an[IllegalArgumentException]
        exception.getMessage shouldBe s"${ChangeDate.typeName} cannot be in the future"
      }
    }
  }
}

class MigrationMessageSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "MigrationMessage" should {

    "have the NonBlank constraint" in {
      MigrationMessage shouldBe an[NonBlank]
    }

    "be instantiatable from any non-blank string" in {
      forAll(nonEmptyStrings()) { body =>
        MigrationMessage.from(body).map(_.value) shouldBe Right(body)
      }
    }

    "be instantiatable from an exception and contain the stack trace" in {
      forAll(nestedExceptions) { exception =>
        MigrationMessage(exception).value shouldBe ErrorMessage.withStackTrace(exception).value
      }
    }
  }
}
