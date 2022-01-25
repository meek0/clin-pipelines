package bio.ferlab.clin

import bio.ferlab.clin.etl.conf.Conf
import bio.ferlab.clin.etl.s3.S3Utils
import cats.FlatMap
import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.implicits._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.services.s3.S3Client

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

package object etl {
  val LOGGER: Logger = LoggerFactory.getLogger(getClass)
  type ValidationResult[A] = ValidatedNel[String, A]

  def withConf[T](b: Conf => ValidationResult[T]): ValidationResult[T] = {
    Conf.readConf().andThen(b)
  }

  def withLog[T](b: ValidationResult[T]): ValidationResult[T] = {
    try {
      b match {
        case Invalid(NonEmptyList(h, t)) =>
          LOGGER.error(h)
          t.foreach(LOGGER.error)
        case Validated.Valid(_) => LOGGER.info("Success!")
      }
      b
    } catch {
      case e: Exception =>
        LOGGER.error("An exception occurerd", e)
        throw e
    }
  }

  def withSystemExit[T](b: ValidationResult[T]): Unit = {
    try {
      b match {
        case Invalid(_) => System.exit(-1)
        case _ => ()
      }
    } catch {
      case _: Exception =>
        System.exit(-1)
    }
  }

  def withReport[T](inputBucket: String, inputPrefix: String)(b: String => ValidationResult[T])(implicit s3Client: S3Client): ValidationResult[T] = {
    val dateTimePart = LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    val reportPath = s"$inputPrefix/logs/$dateTimePart"

    val errorFilePath = s"$reportPath/error.txt"
    try {
      val result = b(reportPath)
      result match {
        case Invalid(NonEmptyList(h, t)) =>

          S3Utils.writeContent(inputBucket, errorFilePath, (h :: t).mkString("\n"))
        case Validated.Valid(_) =>
          val successFilePath = s"$reportPath/success.txt"
          S3Utils.writeContent(inputBucket, successFilePath, "SUCCESS!")
      }
      result
    } catch {
      case e: Exception =>
        S3Utils.writeContent(inputBucket, errorFilePath, ExceptionUtils.getStackTrace(e))
        throw e
    }

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

  def withExceptions[A](f: => A): ValidationResult[A] = {
    Try(f) match {
      case Success(a) =>
        a.validNel
      case Failure(e) =>
        LOGGER.error("Exception occured", e)
        e.getMessage.invalidNel
    }
  }

  implicit val vr: FlatMap[ValidationResult] = new FlatMap[ValidationResult] {
    override def flatMap[A, B](fa: ValidationResult[A])(f: A => ValidationResult[B]): ValidationResult[B] = fa match {
      case Valid(a) => f(a)
      case Invalid(e) => e.invalid[B]
    }


    override def map[A, B](fa: ValidationResult[A])(f: A => B): ValidationResult[B] = fa.map(f)

    @tailrec
    override def tailRecM[A, B](a: A)(f: A => ValidationResult[Either[A, B]]): ValidationResult[B] = f(a) match {
      case Invalid(e) => e.invalid[B]
      case Valid(Left(a)) => tailRecM(a)(f)
      case Valid(Right(a)) => a.validNel[String]
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