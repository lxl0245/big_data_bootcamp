import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object index {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("word_index").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    val text_0_file = sc.textFile("/home/student5/lixinli/week05/data/0.txt")
    val text_1_file = sc.textFile("/home/student5/lixinli/week05/data/1.txt")
    val text_2_file = sc.textFile("/home/student5/lixinli/week05/data/2.txt")

    val words_freq_0 = text_0_file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    val words_freq_1 = text_1_file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    val words_freq_2 = text_2_file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)

    val file_words_freq_0 = words_freq_0.map(item => (item._1, (0, item._2)))
    val file_words_freq_1 = words_freq_1.map(item => (item._1, (1, item._2)))
    val file_words_freq_2 = words_freq_2.map(item => (item._1, (2, item._2)))


    val file_words_freq = file_words_freq_0.union(file_words_freq_1).union(file_words_freq_2)

    file_words_freq.groupByKey().collect().foreach(println)
  }
}
