package bio.ferlab.clin

import bio.ferlab.clin.etl.conf.Conf
import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.implicits._

package object etl {
  type ValidationResult[A] = Validated[NonEmptyList[String], A]

  def withConf[T](b: Conf => ValidationResult[T]): Unit = {
    val result = Conf.readConf().andThen { conf =>
      b(conf)
    }
    result match {
      case Invalid(NonEmptyList(h, t)) =>
        println(h)
        t.foreach(println)
        System.exit(-1)
      case Validated.Valid(_) => println("Success!")
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
