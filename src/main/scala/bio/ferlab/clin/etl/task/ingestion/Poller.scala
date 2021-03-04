package bio.ferlab.clin.etl.task.ingestion

import java.util.concurrent.{ScheduledExecutorService, ScheduledThreadPoolExecutor, ThreadFactory}
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * inspired from:
 * https://github.com/swaldman/mchange-commons-scala/blob/master/src/main/scala/com/mchange/sc/v2/concurrent/Poller.scala
 */
object Poller {
  class PollerException(message: String, cause: Throwable = null) extends Exception(message, cause)
  final class TimeoutException(label: String, deadline: Long) extends PollerException(s"Poller.Task '${label}' expired at ${new java.util.Date(deadline)}")
  final class ClosedException(instance: Poller) extends PollerException(s"Poller '${instance}' has been closed.")

  lazy val DefaultExecutor: ScheduledThreadPoolExecutor = {
    val threadFactory = new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val out = new Thread(r)
        out.setDaemon(true)
        out.setName("clin.etl.task.ingestion.Poller")
        out
      }
    }
    val ses = new ScheduledThreadPoolExecutor(scala.math.round(1.5f * Runtime.getRuntime.availableProcessors()))
    ses.setThreadFactory(threadFactory)
    ses
  }

  implicit lazy val Default: Poller = new PollerWithScheduledExecutorService(DefaultExecutor)

  object Task {

    object withDeadline {
      def apply[T](task: Task[T]): Task.withDeadline[T] = {
        val deadline = if (task.timeout == Duration.Inf) -1 else System.currentTimeMillis + task.timeout.toMillis
        this.apply(task, deadline)
      }
    }

    final case class withDeadline[T] (task: Task[T], deadline: Long) {
      def timedOut: Boolean = deadline >= 0 && System.currentTimeMillis > deadline
    }

    def apply[T](label: String, period: Duration, pollFor: () => Option[T], timeout: Duration = Duration.Inf) =
      new Task(label, period, pollFor, timeout)
  }

  class Task[T](val label: String, val period: Duration, val pollFor: () => Option[T], val timeout: Duration = Duration.Inf) {
    override def toString: String = s"""Poller.Task(label="${label}", period=${period}, timeout=${timeout}"""
  }
}

trait Poller {
  def addTask[T](task: Poller.Task[T]): Future[T]
}
