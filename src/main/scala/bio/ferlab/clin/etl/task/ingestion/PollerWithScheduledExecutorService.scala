package bio.ferlab.clin.etl.task.ingestion

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal


/**
 * inspired from:
 * https://github.com/swaldman/mchange-commons-scala/blob/master/src/main/scala/com/mchange/sc/v2/concurrent/ScheduledExecutorServicePoller.scala
 */
class PollerWithScheduledExecutorService(val ses: ScheduledExecutorService) extends Poller {

  def addTask[T](task: Poller.Task[T]): Future[T] = {

    val promise = Promise[T]()
    scheduleTask(Poller.Task.withDeadline(task), promise)
    promise.future
  }

  private def scheduleTask[T](twd: Poller.Task.withDeadline[T], promise: Promise[T]): Unit = {
    val task = twd.task
    val deadline = twd.deadline

    println(s"Polling: ${twd.task} - timeout in: ${deadline - System.currentTimeMillis}ms")

    val runnable = new Runnable {
      def run(): Unit = {
        try {
          if (! twd.timedOut) {
            task.pollFor() match {
              case Some(value) => promise.success(value)
              case None        => scheduleTask(twd, promise)
            }
          } else {
            promise.failure(new Poller.TimeoutException(task.label, deadline))
          }
        }
        catch {
          case NonFatal(unexpected) => promise.failure(unexpected)
        }
      }
    }
    val millis = task.period.toMillis
    ses.schedule(runnable, millis, TimeUnit.MILLISECONDS)
  }
}


