package ignition.core.utils

import java.nio.ByteBuffer
import java.nio.charset.Charset

object ByteUtils {
  def toString(bytes: Array[Byte], start: Int, length: Int, encoding: String): String = {
    val decoder = Charset.forName(encoding).newDecoder
    decoder.decode(ByteBuffer.wrap(bytes, start, length)).toString
  }

  def toString(bytes: Array[Byte], encoding: String): String = {
    toString(bytes, 0, bytes.length, encoding)
  }

}
