
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext,SparkConf}
/**
 * Created by kexu on 15-8-1.
 */




object mllibNBTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mllibNBTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //读入数据
    val data = sc.textFile("/home/kexu/朴素贝叶斯算法测试数据(2)")
    val parsedData =data.map { line =>
      val parts =line.split(',')
      LabeledPoint(parts(0).toInt,Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }
    // 把数据的70%作为训练集，30%作为测试集.
    val splits = parsedData.randomSplit(Array(0.7,0.3),seed = 11L)
    val training =splits(0)
    val test =splits(1)
    //获得训练模型,第一个参数为训练数据，第二个参数为平滑参数，默认为1，可自定义
    val model =NaiveBayes.train(training,lambda = 1.0)
    //对模型进行准确度分析,得出计算精度
    val predictionAndLabel= test.map(p => (model.predict(p.features),p.label))
    val accuracy =1.0 *predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("accuracy-->"+accuracy)
    //输入(0,0,1,2)的人物特征数据来计算他的性别并输出
    println("Predictionof (1 1 0 0):"+model.predict(Vectors.dense(1,1,0,0)))






  }
}
