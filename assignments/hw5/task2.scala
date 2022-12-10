import java.io._
import scala.collection.mutable.MutableList
import scala.io.Source
import scala.math.pow

object task2 {
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
    //val stream_size = 300
    //val num_of_asks = 30
    //val output_path = "./out.txt"

    val s_t = System.nanoTime
    val bx = new Blackbox
    val p = 1e9 + 7

    def myhashs(user: String): Array[Int] = {
      var result: Array[Int] = Array()
      val hash_funcs: MutableList[List[Int]] = MutableList()
      var a = 0
      var b = 0
      for (i <- 0 to 49) {
        a = 1 + r1.nextInt(997).abs
        b = 1 + r1.nextInt(997).abs
        //println(a, b)
        hash_funcs += List(a, b)
      }
      val x = user.hashCode.abs % 99991
      for (hash <- hash_funcs) {
        result = result :+ (((hash(0) * x + hash(1)) % p) % 997).toInt
      }
      return result
    }

    var result_str = "Time,Ground Truth,Estimation\n"
    var gt_t:Int = 0
    var est_t:Int = 0

    for (i <- 0 to num_of_asks - 1) {
      //println(i)
      val stream_users = bx.ask(input_path, stream_size)
      var gt: Set[String] = Set()
      var exist_hash: MutableList[List[Int]] = MutableList()
      for (user <- stream_users) {
        var result = myhashs(user)
        //println(result.toList)
        if (!gt.contains(user)) {
          gt += user
        }
        exist_hash += result.toList
      }
      //println(exist_hash)
      var est_sum = 0
      for (j <- 0 to 49) {
        var temp: List[Int] = List()
        for (value <- exist_hash) {
          //println(value(j))
          temp = temp :+ value(j)
        }
        //println(temp)
        var max_t_zero = 0
        for (value <- temp) {
          val temp_str = value.toBinaryString
          var zeros = 0
          //println(temp_str.last)
          if (temp_str.last.toString == "1") {
            zeros = 0
          }
          else {
            zeros = temp_str.split("1").last.length
          }
          //println(temp_str, zeros)
          if (max_t_zero < zeros) {
            max_t_zero = zeros
          }
        }
        //println(max_t_zero)
        est_sum += pow(2.0, max_t_zero.toDouble).toInt
      }
      //println("est_sum: ", est_sum)
      val est:Int = (est_sum / 50).toInt
      gt_t += (gt.size)
      est_t += est
      result_str = result_str + i.toString + "," + gt.size.toString + "," + est.toString + "\n"
    }
    val file = new File(output_path)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(result_str)
    bw.close()
    println(est_t.toDouble/gt_t.toDouble)
    val duration = (System.nanoTime - s_t) / 1e9d
    println("Duration: "+ duration)
  }
}