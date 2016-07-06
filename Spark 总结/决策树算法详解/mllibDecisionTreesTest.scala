import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by kexu on 15-8-4.
 */


import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

object mllibDecisionTreesTest {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mllibDecisionTreesTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // Load and parse the data file.
   // val data = MLUtils.loadLibSVMFile(sc, "/home/kexu/下载/spark-1.4.1/data/mllib/sample_libsvm_data.txt")
    val data =MLUtils.loadLibSVMFile(sc,"/home/kexu/桌面/libsvm1")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // 训练一个决策树模型
    //  空 categoricalFeaturesInfo 指示所有特征是连续的.
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "variance"  //方差
    val maxDepth = 5   //最大树深度
    val maxBins = 32   //最大的划分数

    //val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity,
     // maxDepth, maxBins)
    val model = DecisionTree.trainClassifier(trainingData, 2, categoricalFeaturesInfo,
      "gini", maxDepth, maxBins)

    // 测试实例的评估模型和测试误差
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testMSE = labelsAndPredictions.map { case (v, p) => math.pow((v - p), 2) }.mean()
    println("Test Mean Squared Error = " + testMSE)
    println("Learned regression tree model:\n" + model.toDebugString)

    // Save and load model
   // model.save(sc, "myModelPath3")
   // val sameModel = DecisionTreeModel.load(sc, "myModelPath3")

  }
}