package sample

// ライブラリのインポート
import org.apache.spark.{SparkConf, SparkContext}

object BasicFilter {
    def main(args: Array[String]) {
        // SparkContext の作成
        val conf = new SparkConf().setAppName("Basic Filter")
        val sc = new SparkContext(conf)

        // $Spark_HOME を環境に合わせた値に変更
        val textFile = sc.textFile(sys.env("SPARK_HOME") + "/README.md")
        val filteredLine = textFile.filter(line => line.contains("Scala"))

        // 処理結果の出力
        filteredLine.saveAsTextFile("./filter")
    }
}
