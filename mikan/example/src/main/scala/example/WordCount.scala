package example

import org.apache.spark.{SparkContext, SparkConf}

object WordCount {
    def main(args: Array[String]): Unit = {
        // SparkContext の作成
        val conf = SparkConf().setAppName("WordCount")
        val sc = new SparkContext(conf)

        // 処理の対象となる文書をスペースで区切って単語の配列をつくる
        val sourceText = "Hello Spark, this is my first Spark application."
        val words = sourceText.split(" ").map(_.replaceAll("[.,]",""))

        // 配列から最初の RDD (firstRdd) を生成
        val firstRdd = sc.parallelize(words)

        // RDD に対してトランスフォーメーションを適用していく
        val secondRdd = firstRdd.map(item => (item, 1))
        val finalRdd = secondRdd.reduceBykey((x, y) => x + y)

        // アクションの実効
        val result = finalRdd.collect()

        result.foreach(i => println(i))
    }
}
