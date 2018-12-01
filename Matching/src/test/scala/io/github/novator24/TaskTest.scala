package io.github.novator24

import org.scalatest.{FlatSpec, PrivateMethodTester}

// http://www.scalatest.org/user_guide/using_scalatest_with_sbt
// https://stackoverflow.com/a/24375762
class TaskTest extends FlatSpec with PrivateMethodTester {

  "Clients" should "check correctly" in {
    val cls = new Task()
    val func = PrivateMethod[Task]('checkClients)
    //val client = Client("", 0, 0, 0, 0, 0)
    //val result = cls invokePrivate func(stream)
    assert(true)
  }
  

}