package org.novelfs.streaming.kafka

import fs2._
import cats.implicits._

import scala.concurrent.duration.FiniteDuration

package object ops {
  implicit class ExtraStreamOps[F[_], O](s : Stream[F, O]) {
    def takeElementsEvery(d : FiniteDuration) =
      s.tail.zip(Stream.every(d))
        .filterWithPrevious((t1, t2) => t1._2 != t2._2)
        .map(_._1)


    def suppressErrorWith(f : PartialFunction[Throwable, F[Unit]]): Stream[F, O] =
      s.handleErrorWith(t => f.lift(t) match {
        case Some(act) => Stream.eval(act).flatMap(_ => Stream.empty)
        case None => Stream.raiseError(t)
      })
  }


}