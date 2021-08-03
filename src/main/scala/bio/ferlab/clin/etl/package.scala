package bio.ferlab.clin

import bio.ferlab.clin.etl.conf.Conf
import bio.ferlab.clin.etl.s3.S3Utils
import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.implicits._
import software.amazon.awssdk.services.s3.S3Client

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

package object etl {
  type ValidationResult[A] = Validated[NonEmptyList[String], A]

  def withConf[T](b: Conf => ValidationResult[T]): ValidationResult[T] = {
    Conf.readConf().andThen(b)
  }

  def withLog[T](b: => ValidationResult[T]): ValidationResult[T] = {
    b match {
      case Invalid(NonEmptyList(h, t)) =>
        println(h)
        t.foreach(println)
      case Validated.Valid(_) => println("Success!")
    }
    b
  }

  def withSystemExit[T](b: => ValidationResult[T]): Unit = {
    b match {
      case Invalid(_) => System.exit(-1)
      case _ => ()
    }
  }

  def withReport[T](inputBucket: String, inputPrefix: String)(b: String => ValidationResult[T])(implicit s3Client: S3Client): ValidationResult[T] = {
    val dateTimePart = LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    val reportPath = s"$inputPrefix/logs/$dateTimePart"
    val result = b(reportPath)
    result match {
      case Invalid(NonEmptyList(h, t)) =>
        val errorFilePath = s"$reportPath/error.txt"
        S3Utils.writeContent(inputBucket, errorFilePath, (h :: t).mkString)
      case Validated.Valid(_) =>
        val successFilePath = s"$reportPath/success.txt"
        S3Utils.writeContent(inputBucket, successFilePath, "SUCCESS!")
    }
    result

  }

  def allValid[A, E, T](v: ValidatedNel[A, E]*)(f: => T): Validated[NonEmptyList[A], T] = {
    v.toList.sequence_.map(_ => f)
  }

  def isValid[A, E](f: => A, errors: Seq[E]): ValidatedNel[E, A] = {
    errors match {
      case Nil => f.validNel[E]
      case s => NonEmptyList.fromList(s.toList).get.invalid[A]
    }
  }

  implicit class ValidatedNelExtension[A, B](v1: ValidatedNel[A, B]) {
    def appendErrors(v2: ValidatedNel[A, _]): ValidatedNel[A, B] = {
      (v1, v2) match {
        case (Valid(_), i2@Invalid(_)) => i2
        case (Invalid(i1), Invalid(i2)) => Invalid(i1 ::: i2)
        case _ => v1
      }
    }


  }


}
