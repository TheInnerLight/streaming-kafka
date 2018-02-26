package org.novelfs.streaming.kafka

import java.nio.ByteBuffer

object TestHelpers {
  def byteArrayToIntArray(byteArray : Array[Byte]) = {
    val buffer = ByteBuffer.wrap(byteArray.clone()).asIntBuffer()
    val arr = Array.fill(buffer.limit())(0)
    buffer.get(arr)
    arr
  }
}
