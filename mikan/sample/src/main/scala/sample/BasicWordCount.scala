package sample

// ライブラリのインポート
import org.apache.spark.{SparkConf, SparkContext}

object BasicWordCount {
    def main(args: Array[String]) {
        
        // SparkContext の作成
        val conf = new SparkConf().setAppName("Basic WordCount")
        val sc = new SparkContext(conf)

        // $Spark_HOME を環境に合わせた値に変更
        val textFile = sc.textFile(sys.env("SPARK_HOME") + "/README.md")
        val words = textFile.flatMap(line => line.split(" "))
        val wordCounts = words.map(word => (word, 1)).reduceByKey((a, b) => a + b)

        // 処理結果の出力
        wordCounts.saveAsTextFile("./wordcounts")
    }
}

