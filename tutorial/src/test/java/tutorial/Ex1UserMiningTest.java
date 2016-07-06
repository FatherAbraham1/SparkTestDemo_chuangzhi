//package tutorial;
//
//
//import utils.Tweet;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.function.Function;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//
//import scala.Tuple2;
//
//
//public class Ex1UserMiningTest {
//
//  private Ex1UserMining ex1UserMining;
//
//  @Before
//  public void init() {
//    ex1UserMining = new Ex1UserMining();
//  }
//
//  @Test
//  public void tweetsByUser() {
//    // run
//    JavaPairRDD<String, Iterable<Tweet>> tweetsByUser = ex1UserMining.tweetsByUser();
//
//    // assert
//    Assert.assertEquals(5967, tweetsByUser.count());
//  }
//
//  @Test
//  public void tweetByUserNumber() {
//    // run
//    JavaPairRDD<String, Integer> result = ex1UserMining.tweetByUserNumber();
//
//    // assert
//    Assert.assertEquals(5967, result.count());
//
//    JavaPairRDD<String, Integer> example = result.filter(new Function<Tuple2<String, Integer>, Boolean>() {
//		public Boolean call(Tuple2<String, Integer> tuple) throws Exception {
//			return "Dell Feddi".equals(tuple._1());
//		}
//	});
//    Assert.assertEquals(1, example.count());
//    Assert.assertEquals(29, example.collect().get(0)._2().intValue());
//  }
//
//}
