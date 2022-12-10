import java.io._
import scala.collection.mutable.MutableList
import scala.io.Source

object task1 {
  class Blackbox {
    private val r1 = scala.util.Random
    r1.setSeed(553)
    def ask(filename: String, num: Int): Array[String] = {
      val input_file_path = filename

      val lines = Source.fromFile(input_file_path).getLines().toArray
      var stream = new Array[String](num)

      for (i <- 0 to num - 1) {
        stream(i) = lines(r1.nextInt(lines.length))
      }
      return stream
    }
  }

  def main(arg:Array[String]): Unit={

    val r1 = scala.util.Random
    r1.setSeed(553)
    val input_path = arg(0)
    val stream_size = arg(1).toInt
    val num_of_asks = arg(2).toInt
    val output_path = arg(3)

    //val input_path = "./users.txt"
    //val stream_size = 100
    //val num_of_asks = 30
    //val output_path = "./out.txt"

    val s_t = System.nanoTime
    val bx = new Blackbox
    val p = 1e9 + 7
    var exist_users: Set[String] = Set()
    var filter = (for (i <- 0 to 69996) yield 0).toList

    def myhashs(user: String): Array[Int] = {
      var result: Array[Int] = Array()
      val hash_funcs: MutableList[List[Int]] = MutableList()
      var a = 0
      var b = 0
      for (i <- 0 to 29) {
        a = 1 + r1.nextInt(999).abs
        b = 1 + r1.nextInt(999).abs
        //println(a, b)
        hash_funcs += List(a, b)
      }
      val x = user.hashCode.abs % 99991
      for (hash <- hash_funcs) {
        result = result :+ (((hash(0) * x + hash(1)) % p) % 69997).toInt
      }
      return result
    }

    var result_str = "Time,FPR\n"
    var fpr_list: List[Double] = List()
    for (i <- 0 to num_of_asks - 1) {
      //println(i)
      val stream_users = bx.ask(input_path, stream_size)
      var fp: Int = 0
      var n: Int = 0
      for (user <- stream_users) {
        var result = myhashs(user)
        //println(result.toList)
        if (!exist_users.contains(user)) {
          n += 1
          var flag = false
          for (value <- result) {
            if (filter(value) == 0) {
              flag = true
            }
          }
          if (flag == false) {
            fp += 1
          }
        }
        for (value <- result) {
          filter = filter.updated(value, 1)
        }
      }
      var fpr:Double = fp / n
      var temp = exist_users.toList ++ stream_users
      exist_users = temp.toSet
      //println(exist_users.size)
      fpr_list = fpr_list ++ List(fpr)
      result_str = result_str + i.toString + "," + fpr.toString + "\n"
    }
    val file = new File(output_path)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(result_str)
    bw.close()
    //println(fpr_list)
    val duration = (System.nanoTime - s_t) / 1e9d
    println("Duration: "+ duration)
  }
}