import java.io._
import scala.collection.mutable.MutableList
import scala.io.Source

object task3 {
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
    //val num_of_asks = 100
    //val output_path = "./out1.txt"

    val s_t = System.nanoTime
    val bx = new Blackbox

    var exist_users: MutableList[String] = MutableList()

    var result_str = "seqnum,0_id,20_id,40_id,60_id,80_id\n"
    var n: Float = 0
    for (i <- 0 to num_of_asks - 1) {
      //println(i)
      val stream_users = bx.ask(input_path, stream_size)
      for (user <- stream_users) {
        n += 1
        if (exist_users.length < 100) {
          exist_users += user
        }
        else {
          //println(exist_users.length)
          if (r1.nextFloat() < 100/n) {
            val temp = r1.nextInt(100)
            println(temp)
            exist_users(temp) = user
          }
        }
        if (n % 100 == 0) {
          result_str = result_str + n.toInt.toString + "," + exist_users(0) + ',' + exist_users(20) + ',' + exist_users(40) + ',' + exist_users(60) + ',' + exist_users(80) + "\n"
        }
      }
    }
    val file = new File(output_path)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(result_str)
    bw.close()
    val duration = (System.nanoTime - s_t) / 1e9d
    println("Duration: "+ duration)
  }
}