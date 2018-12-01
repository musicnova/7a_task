package io.github.novator24

import java.io.File
import com.github.tototoshi.csv._

class Task {
  // README https://github.com/tototoshi/scala-csv
  implicit class StringImprovements(val s: String) {
    import scala.util.control.Exception._
    def toBigIntOpt = catching(classOf[NumberFormatException]) opt BigInt(s)
    def toCharOpt = catching(classOf[NumberFormatException]) opt s(0)
  }

  case class Client(name: String
                    , total_usd: Option[BigInt]
                    , total_a_units: Option[BigInt]
                    , total_b_units: Option[BigInt]
                    , total_c_units: Option[BigInt]
                    , total_d_units: Option[BigInt]) {}

  def readClients(filename: String): Stream[Client] = {
    implicit object ClientFormat extends DefaultCSVFormat {
      override val delimiter = '\t'
    }
    CSVReader.open(new File(filename)).toStream.map((x:List[String]) => Client(x.head
      , x(1).toBigIntOpt
      , x(2).toBigIntOpt
      , x(3).toBigIntOpt
      , x(4).toBigIntOpt
      , x(5).toBigIntOpt))
  }

  case class Order(clientName: String
                    , letter: Option[Char]
                    , emitName: String
                    , unitPrice: Option[BigInt]
                    , orderSize: Option[BigInt]) {}
  def readOrders(filename: String): Stream[Order] = {
    implicit object OrderFormat extends DefaultCSVFormat {
      override val delimiter = '\t'
    }
    CSVReader.open(new File(filename)).toStream.map((x:List[String]) => Order(x.head
      , x(1).toCharOpt, x(2), x(3).toBigIntOpt, x(4).toBigIntOpt))
  }

  def checkClients(clients: Stream[Client]): Stream[Client] = {
    clients.filter(_.name.nonEmpty)
  }

  def checkOrders(orders: Stream[Order]): Stream[Order] = {
    orders.filter(_.clientName.nonEmpty)
  }

  case class Result(key: String, client: Option[Client], order: Option[Order]) {}
  def calcClientsXOrders(clients: Stream[Client]
                         , orders: Stream[Order]): Stream[Result] = {
    val clientsMap = clients.groupBy(_.name)
    val ordersMap = orders.groupBy(_.clientName)
    clientsMap.keys.toStream.map(k => {
      val c = clientsMap(k)
      if (c.size > 1) throw new Exception("dup name "+k)
      var res = c.head
      for (o <- ordersMap(k)) {
        if (o.letter == Option[Char]('s')) {
          if(o.emitName == "A") {
            res = Client(res.name
              , Option[BigInt](res.total_usd.get + o.unitPrice.get * o.orderSize.get)
              , Option[BigInt](res.total_a_units.get - o.orderSize.get)
              , Option[BigInt](res.total_b_units.get)
              , Option[BigInt](res.total_c_units.get)
              , Option[BigInt](res.total_d_units.get)
            )
          }
          if(o.emitName == "B") {
            res = Client(res.name
              , Option[BigInt](res.total_usd.get + o.unitPrice.get * o.orderSize.get)
              , Option[BigInt](res.total_a_units.get)
              , Option[BigInt](res.total_b_units.get - o.orderSize.get)
              , Option[BigInt](res.total_c_units.get)
              , Option[BigInt](res.total_d_units.get)
            )
          }
          if(o.emitName == "C") {
            res = Client(res.name
              , Option[BigInt](res.total_usd.get + o.unitPrice.get * o.orderSize.get)
              , Option[BigInt](res.total_a_units.get)
              , Option[BigInt](res.total_b_units.get)
              , Option[BigInt](res.total_c_units.get - o.orderSize.get)
              , Option[BigInt](res.total_d_units.get)
            )
          }
          if(o.emitName == "D") {
            res = Client(res.name
              , Option[BigInt](res.total_usd.get + o.unitPrice.get * o.orderSize.get)
              , Option[BigInt](res.total_a_units.get)
              , Option[BigInt](res.total_b_units.get)
              , Option[BigInt](res.total_c_units.get)
              , Option[BigInt](res.total_d_units.get - o.orderSize.get)
            )
          }
        }
        if (o.letter == Option[Char]('b')) {
          if(o.emitName == "A") {
            res = Client(res.name
              , Option[BigInt](res.total_usd.get - o.unitPrice.get * o.orderSize.get)
              , Option[BigInt](res.total_a_units.get + o.orderSize.get)
              , Option[BigInt](res.total_b_units.get)
              , Option[BigInt](res.total_c_units.get)
              , Option[BigInt](res.total_d_units.get)
            )
          }
          if(o.emitName == "B") {
            res = Client(res.name
              , Option[BigInt](res.total_usd.get - o.unitPrice.get * o.orderSize.get)
              , Option[BigInt](res.total_a_units.get)
              , Option[BigInt](res.total_b_units.get + o.orderSize.get)
              , Option[BigInt](res.total_c_units.get)
              , Option[BigInt](res.total_d_units.get)
            )
          }
          if(o.emitName == "C") {
            res = Client(res.name
              , Option[BigInt](res.total_usd.get - o.unitPrice.get * o.orderSize.get)
              , Option[BigInt](res.total_a_units.get)
              , Option[BigInt](res.total_b_units.get)
              , Option[BigInt](res.total_c_units.get + o.orderSize.get)
              , Option[BigInt](res.total_d_units.get)
            )
          }
          if(o.emitName == "D") {
            res = Client(res.name
              , Option[BigInt](res.total_usd.get - o.unitPrice.get * o.orderSize.get)
              , Option[BigInt](res.total_a_units.get)
              , Option[BigInt](res.total_b_units.get)
              , Option[BigInt](res.total_c_units.get)
              , Option[BigInt](res.total_d_units.get + o.orderSize.get)
            )
          }
        }
      }
      Result(k, Option[Client](res), Option.empty[Order])
    })
  }

  def saveResults(filename: String, results: Stream[Result]): Unit = {
    // README https://github.com/tototoshi/scala-csv
    implicit object OrderFormat extends DefaultCSVFormat {
      override val delimiter = '\t'
    }
    val writer = CSVWriter.open(new File(filename))
    results.foreach(r => {
      writer.writeRow(List(r.client.get.name
        , r.client.get.total_usd.get
        , r.client.get.total_a_units.get
        , r.client.get.total_b_units.get
        , r.client.get.total_c_units.get
        , r.client.get.total_d_units.get))
    })
  }
}

object Task {

  def main(args: Array[String]): Unit = {
    val cls = new Task()
    val dir = "/home/user/GIT/7a_task/Matching"
    val dataClients = cls.readClients(new File(dir, "clients.txt").toString)
    val dataOrders = cls.readOrders(new File(dir, "orders.txt").toString)
    val clients = cls.checkClients(dataClients)
    val orders = cls.checkOrders(dataOrders)
    val results = cls.calcClientsXOrders(clients, orders)
    cls.saveResults(new File(dir, "results.txt").toString, results)
  }

}
