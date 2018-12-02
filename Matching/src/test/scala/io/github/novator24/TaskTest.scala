package io.github.novator24

import org.scalatest.{FlatSpec, PrivateMethodTester}

// http://www.scalatest.org/user_guide/using_scalatest_with_sbt
// https://stackoverflow.com/a/24375762
class TaskTest extends FlatSpec with PrivateMethodTester {

  "Positive01" should "checkClients() correctly" in {
    val cls = new Task()
    val zero = Option[BigInt](0)
    val client = cls.Client("", zero, zero, zero, zero, zero)
    val stream = List(client).toStream
    val result = cls.checkClients(stream)
    assert(result.isEmpty)
  }
  

}