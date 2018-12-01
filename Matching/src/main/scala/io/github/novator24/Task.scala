package io.github.novator24

import java.io.File
import com.github.tototoshi.csv._

class Task {
  case class Client(name: String
                    , total_usd: Double
                    , total_a_units: Double
                    , total_b_units: Double
                    , total_c_units: Double
                    , total_d_units: Double) {}

  def read_clients(filename: String): List[Client] = {
    implicit object ClientFormat extends DefaultCSVFormat {
      override val delimiter = '\t'
    }
    CSVReader.open(new File(filename)).toStream.map((x:List[String]) => Client(x.head
      , x(1).toDouble, x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble)).toList
  }

  case class Order(clientName: String
                    , operation: Char
                    , emitName: String
                    , unitPrice: BigInt
                    , orderSize: BigInt) {}
  def read_orders(filename: String): List[Order] = {
    implicit object OrderFormat extends DefaultCSVFormat {
      override val delimiter = '\t'
    }
    CSVReader.open(new File(filename)).toStream.map((x:List[String]) => Order(x.head
      , x(1)(0), x(2), BigInt(x(3)), BigInt(x(4)))).toList
  }
}


object Task {

  def main(args: Array[String]): Unit = {
    // README https://github.com/tototoshi/scala-csv
    val cls = new Task()
    val dir = "/home/user/GIT/7a_task/Matching"
    val clients = cls.read_clients(new File(dir, "clients.txt").toString)
    val orders = cls.read_orders(new File(dir, "orders.txt").toString)
    print(clients.size)
    print(orders.size)
  }

}
