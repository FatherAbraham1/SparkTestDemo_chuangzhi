//package tutorial;
//
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import scala.Tuple2;
//
//import java.io.Serializable;
//import java.util.List;
//
//public class Ex2TweetMiningTest implements Serializable {
//
//	private Ex2TweetMining ex2TweetMining;
//
//	@Before
//	public void init() {
//		ex2TweetMining = new Ex2TweetMining();
//	}
//
//	@Test
//	public void mentionOnTweet() {
//		// run
//		JavaRDD<String> mentions = ex2TweetMining.mentionOnTweet();
//
//		// assert
//		Assert.assertEquals(4462, mentions.count());
//		JavaRDD<String> filter = mentions.filter(new Function<String, Boolean>() {
//			public Boolean call(String mention) throws Exception {
//				return "@JordinSparks".equals(mention);
//			}
//		});
//		Assert.assertEquals(2, filter.count());
//	}
//
//	@Test
//	public void countMentions() {
//		// run
//		JavaPairRDD<String, Integer> counts = ex2TweetMining.countMentions();
//
//		// assert
//		Assert.assertEquals(3283, counts.count());
//		JavaPairRDD<String, Integer> filter = counts.filter(new Function<Tuple2<String, Integer>, Boolean>() {
//			public Boolean call(Tuple2<String, Integer> couple) throws Exception {
//				return "@JordinSparks".equals(couple._1());
//			}
//		});
//		Assert.assertEquals(1, filter.count());
//		Assert.assertEquals(2, filter.first()._2().intValue());
//	}
//
////	@Test
////	public void top10mentions() {
////		// run
////		List<Tuple2<Integer, String>> mostMentioned = ex2TweetMining.top10mentions();
////
////		// assert
////		Assert.assertEquals(10, mostMentioned.size());
////		Assert.assertEquals(189, mostMentioned.get(0)._1().intValue());
////		Assert.assertEquals("@ShawnMendes", mostMentioned.get(0)._2());
////		Assert.assertEquals(100, mostMentioned.get(1)._1().intValue());
////		Assert.assertEquals("@HIITMANonDECK", mostMentioned.get(1)._2());
////	}
//
//}