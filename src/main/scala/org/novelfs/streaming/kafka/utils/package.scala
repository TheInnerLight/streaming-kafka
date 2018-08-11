package org.novelfs.streaming.kafka

import cats.implicits._
import cats.effect.ConcurrentEffect
import cats.effect.concurrent.MVar

package object utils {
  implicit class MVarOps[F[_] : ConcurrentEffect, A](val mvar : MVar[F, A]) {
    def locked[B](f : A => F[B]): F[B] =
      for {
        item <- mvar.take
        result <- f(item)
        _ <- mvar.put(item)
      } yield result
  }
}
