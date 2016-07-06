package cn.wh.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Mutable;
import scala.Tuple2;

/**
 * @author hadoop
 *
 */
public class Test11 {

	public static String UserPath = "/home/hadoop/data1/ult.txt";
	public static String BlocationPath = "/home/hadoop/data1/businessHall-location.txt";
	public static String JarPath = "/home/hadoop/data1/Test.jar";
	public static long count;
	static SparkConf conf = new SparkConf()
			.setAppName("Test")
			//
			// .setMaster("spark://192.168.3.8:7077")//
			.setMaster("local")
			.set("spark.driver.allowMultipleContexts", "true");
	static JavaSparkContext sc = new JavaSparkContext(conf);

	public static void TestgetLocal() {

		JavaPairRDD<String, Iterable<String>> group = getLocal();
		for (Tuple2<String, Iterable<String>> kv : group.collect()) {
			System.out.println(kv._1() + "        " + kv._2());
		}

	}

	public static void TestGetTime() {
		Scanner sc = new Scanner(System.in);
		System.out.println("输入一个时间范围(分钟):");
		long str = sc.nextLong();

		for (Tuple2<String, Long> kv : getTime(str).collect()) {
			System.out.println(kv._1() + "        " + kv._2());
		}
	}

	public static void TestGetAddressUsers() {
		getAddressUsers();

	}
	
	public static void TestGetUserAddressTimes() {
		//Scanner sc = new Scanner(System.in);
	//	System.out.println("输入一个时间范围(分钟):");
		//long str = sc.nextLong();
		getUserAddressTimes(100L);
	}

	public static void main(String[] args) {
		 TestgetLocal();
		 //TestGetTime();
		//TestGetAddressUsers();
		//TestGetUserAddressTimes();

	}

	public static JavaPairRDD<String, Iterable<String>> getLocal() {
		sc.addJar(JarPath);
		JavaRDD<String> text = sc.textFile(UserPath);
		JavaPairRDD<String, String> line = text
				.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

					@Override
					public Iterable<Tuple2<String, String>> call(String arg0)
							throws Exception {
						String[] str = arg0.split("\\|");
						String id = str[0];
						String local = str[1] + "|" + str[2];
						List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
						list.add(new Tuple2<String, String>(id, local));
						return list;
					}
				});
		// System.out.println(line.first()._1() + " " + line.first()._2());
		JavaPairRDD<String, Iterable<String>> group = line.groupByKey();
		// group.saveAsTextFile("hdfs://master:9000/usr/spark/getlocal");
		return group;
	}

	public static long formDateStringToLong(String string)
			throws ParseException {
		// long time=Timestamp.valueOf(str[3]).getTime()/1000;
		// 2014-07-16 04:03:52
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dt2 = sdf.parse(string);
		// 继续转换得到秒数的long型
		long time = dt2.getTime() / 1000;
		return time;
	}
	
	public static String PrintFormat(Object string){

		return "["+string+"]";
		
	}

	public static JavaPairRDD<String, Long> getTime(Long IntervalTime) {
		sc.addJar(JarPath);
		JavaRDD<String> text = sc.textFile(UserPath);
		// 先对数据分组
		JavaPairRDD<String, Long> line = text
				.flatMapToPair(new PairFlatMapFunction<String, String, Long>() {
					@Override
					public Iterable<Tuple2<String, Long>> call(String words)
							throws Exception {
						String[] str = words.split("\\|");

						long time = formDateStringToLong(str[3]);

						String idlocal = str[0] + "        " + str[1] + "|"
								+ str[2];
						List<Tuple2<String, Long>> list = new ArrayList<Tuple2<String, Long>>();
						list.add(new Tuple2<String, Long>(idlocal, time));

						return list;
					}
				});
		JavaPairRDD<String, Long> dis = line.distinct();
		JavaPairRDD<String, Iterable<Long>> group = dis.groupByKey();
		JavaPairRDD<String, Iterable<Long>> sort = group.sortByKey();

		// 对分组后的数据进行KV对换
		JavaPairRDD<String, Long> timeall = sort
				.mapToPair(new PairFunction<Tuple2<String, Iterable<Long>>, String, Long>() {
					@Override
					public Tuple2<String, Long> call(
							Tuple2<String, Iterable<Long>> userTuple2)
							throws Exception {
						Iterator<Long> list = userTuple2._2().iterator();
						String sp = "";
						while (list.hasNext()) {
							sp += list.next() + "|";
						}
						String[] time = sp.split("\\|");
						long times = 0;

						for (int i = time.length - 1; i >= 0; i--) {
				
							if (i >=1&&i!=0) {
								if (Math.abs(Long.parseLong(time[i])- Long.parseLong(time[i - 1])) < IntervalTime * 60) {
									times += Math.abs(Long.parseLong(time[i])- Long.parseLong(time[i - 1]));
								}else {
									times+=0;
								}
							}

					
						}
						return new Tuple2<String, Long>(userTuple2._1(), times);
					}
				});
		return timeall;
	}

	
	public static void getAddressUsers() {
		sc.addJar(JarPath);
		JavaRDD<String> text = sc.textFile(UserPath);
		JavaRDD<String> local = sc.textFile(BlocationPath);
		// 先对数据分组
		JavaPairRDD<String, String> user = text.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

					@Override
					public Iterable<Tuple2<String, String>> call(String words)
							throws Exception {
						String[] str = words.split("\\|");
						String user = str[0];
						String idlocal = str[1] + "|" + str[2];
						List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
						list.add(new Tuple2<String, String>(idlocal, user));

						return list;
					}
				});

		JavaPairRDD<String, String> address = local.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

					@Override
					public Iterable<Tuple2<String, String>> call(String arg0)
							throws Exception {
						String[] str = arg0.split("\\,");
						String add = "#" + str[0];
						String local = str[1].split("\\.")[0] + "|"
								+ str[2].split("\\.")[0];
						List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
						list.add(new Tuple2<String, String>(local, add));

						return list;
					}
				});
		
		
		JavaPairRDD<String, Tuple2<String, String>> joinRDD = address.join(user).distinct(); 
		JavaPairRDD<String, Iterable<Tuple2<String, String>>> group = joinRDD.groupByKey();

		JavaPairRDD<String, String> result=group.mapToPair(new PairFunction<Tuple2<String,Iterable<Tuple2<String,String>>>, String, String>() {

			@Override
			public Tuple2<String, String> call(
					Tuple2<String, Iterable<Tuple2<String, String>>> v1)
					throws Exception {
				Iterator<Tuple2<String, String>> value = v1._2().iterator();
				String add = "";
				String user = "";
				while (value.hasNext()) {
					Tuple2<java.lang.String, java.lang.String> tuple2 = (Tuple2<java.lang.String, java.lang.String>) value.next();
					add=tuple2._1().replace("#", "");
					user += tuple2._2() + ",";
					
				}

				user=PrintFormat(user);
				
				return new Tuple2<String, String>(add, user);
			}
		});
		
     		for (Tuple2<String, String> tuple2 : result.distinct().collect()) {
     			System.out.println(tuple2._1() + "    " + tuple2._2());
     		}

	}


	public static void getUserAddressTimes(Long IntervalTime) {

		JavaRDD<String> text = sc.textFile(UserPath);
		JavaRDD<String> local = sc.textFile(BlocationPath);
		
		// 先对数据分组
				JavaPairRDD<String, String> user = text.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

							@Override
							public Iterable<Tuple2<String, String>> call(String words)
									throws Exception {
								String[] str = words.split("\\|");
								String user = str[0];
								String idlocal = str[1] + "|" + str[2];
								long time = formDateStringToLong(str[3]);
								String user_time=user+"|"+time;
								List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
								list.add(new Tuple2<String, String>(idlocal, user_time));

								return list;
							}
						});

				JavaPairRDD<String, String> address = local.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

							@Override
							public Iterable<Tuple2<String, String>> call(String lines)
									throws Exception {
								String[] str = lines.split("\\,");
								String add = "#" + str[0];
								String local = str[1].split("\\.")[0] + "|"
										+ str[2].split("\\.")[0];
								List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
								list.add(new Tuple2<String, String>(local, add));

								return list;
							}
						});
				
				JavaPairRDD<String, Tuple2<String, String>> joinRDD = user.join(address).distinct(); 
				JavaPairRDD<String, Iterable<Tuple2<String, String>>> group = joinRDD.groupByKey();

				/**
				 * 
				 * "HUBSC102HWE_HUB0036市区开发区澳普蓝"    254824543|1405498951,257043521|1405511536,291075327|1405503467,278452626|1405473173,291013286|1405477066,279480327|1405452491,60130069|1405489697,286389556|1405512934,261248568|1405475902,239620401|1405523881,1625844|1405480246,281363020|1405510281,281363020|1405490991,255074816|1405494650,25090516|1405455309,281363020|1405480701,275936757|1405456708,290742578|1405495758,287608331|1405512222,273510691|1405468880,281363020|1405490595,281363020|1405483679,239620401|1405521013,284809384|1405453257,282205485|1405469917,291075327|1405481434,6417976|1405466390,6133300|1405459414,95324069|1405524339,289347253|1405502855,281363020|1405489744,275936757|1405453827,291075327|1405468127,239620401|1405475077,279480327|1405455078,281363020|1405493164,290674351|1405490155,267885014|1405500888,282205485|1405488555,267885014|1405491362,289703937|1405477832,282205485|1405471640,270249354|1405471490,284081147|1405464267,286739335|1405524311,279480327|1405464054,281363020|1405469170,290742578|1405491997,2725397|1405487996,2725397|1405474091,284698100|1405474363,286466236|1405515836,279480327|1405463221,269913669|1405453324,291075327|1405481202,274979067|1405454399,40001644|1405498094,273510691|1405467027,239609567|1405466564,254824543|1405479190,248814261|1405490689,279480327|1405464758,254824543|1405480023,239620401|1405483657,282205485|1405482977,273510691|1405494526,257043521|1405454410,290674351|1405481508,286976510|1405516953,281363020|1405479496,281363020|1405499393,291013286|1405482984,291013286|1405483393,281363020|1405498255,275936757|1405504763,288436080|1405454582,291075327|1405468858,259135204|1405501470,2725397|1405489705,282205485|1405477695,273510691|1405495776,275936757|1405465352,273510691|1405493212,290345503|1405476114,281363020|1405505469,289347253|1405479920,274979067|1405514904,281363020|1405484074,274979067|1405457280,291075327|1405481283,284809384|1405456139,206830104|1405498884,42797276|1405463794,254824543|1405485139,291075327|1405468202,25090516|1405458190,239620401|1405495111,265570871|1405492572,291075327|1405490645,274279076|1405489824,291075327|1405501816,279480327|1405454652,240948430|1405476944,288436080|1405457464,291075327|1405510006,269913669|1405491974,60130069|1405486816,279480327|1405457959,282205485|1405510766,234045588|1405488649,281363020|1405475466,28566737|1405472182,269913669|1405518493,2725397|1405485518,281363020|1405483446,269913669|1405517757,239620401|1405509381,291013286|1405470176,208228295|1405496270,259366617|1405484140,291075327|1405468083,291075327|1405501413,254941594|1405470646,281363020|1405468998,206678009|1405459036,287278609|1405487311,285659629|1405452869,282205485|1405469430,239620401|1405472212,291075327|1405512319,95324069|1405518575,291075327|1405469313,281363020|1405509299,40001644|1405469893,273510691|1405494673,258846518|1405473162,282205485|1405468187,239620401|1405515287,201708552|1405475484,281363020|1405485463,254824543|1405485271,281363020|1405500981,25090516|1405461071,71855879|1405458944,279995254|1405467148,291075327|1405481378,269913669|1405493577,257043521|1405463913,239620401|1405477936,257043521|1405463820,286389556|1405475922,273510691|1405473671,282205485|1405478568,281363020|1405502898,276926665|1405503085,287014708|1405501451,79275908|1405500126,269913669|1405459088,275936757|1405459589,281363020|1405496669,282205485|1405507912,282205485|1405493382,288384431|1405470457,257043521|1405457291,281363020|1405468447,201708552|1405488581,291075327|1405479442,11024530|1405475233,281363020|1405487951,239620401|1405497994,273510691|1405470398,1625844|1405485682,208228295|1405500843,5114514|1405483655,40001644|1405464131,95324069|1405521457,281363020|1405490868,282205485|1405473211,284698100|1405474407,286747543|1405486745,206466499|1405510608,239620401|1405518140,208228295|1405473237,291075327|1405469204,254824543|1405478634,257043521|1405463177,274979067|1405483211,281363020|1405473775,254824543|1405485731,282205485|1405471934,291075327|1405467961,279480327|1405460340,291075327|1405505888,206830104|1405500999,289347253|1405499988,275936757|1405462471,95324069|1405460438,279480327|1405453857,281363020|1405474408,279480327|1405453684,291013286|1405493913,239620401|1405489379,291075327|1405468413,286389556|1405498974,291075327|1405481559,273510691|1405494746,286389556|1405490329,291013286|1405472568,208228295|1405481830,66721805|1405475981,281926379|1405454005,281363020|1405479348,239620401|1405480816,
				 * */
				JavaPairRDD<String, String> result_Add_users=group.mapToPair(new PairFunction<Tuple2<String,Iterable<Tuple2<String,String>>>, String, String>() {

					@Override
					public Tuple2<String, String> call(
							Tuple2<String, Iterable<Tuple2<String, String>>> v1)
							throws Exception {
						Iterator<Tuple2<String, String>> value = v1._2().iterator();
						String add = "";
						String user = "";
						while (value.hasNext()) {
							Tuple2<java.lang.String, java.lang.String> tuple2 = (Tuple2<java.lang.String, java.lang.String>) value.next();
							add=tuple2._2().replace("#", "");
							//add=tuple2._2();
							user+=tuple2._1()+",";
						}
						return new Tuple2<String, String>(user, add);
					}
				});
				
				/**
				 * 254884247|1405521563    "HUBSC94HWE_HUB2229长兴开发区永能化纤"
					286140887|1405456878    "HUBSC97HWE_HUB3173德清钟管青墩村委"
					201893043|1405508667    "HUBSC94HWE_HUD2166长兴财政局1800"
				 * */
				 JavaPairRDD<String, String> pairs = result_Add_users.flatMapToPair(new PairFlatMapFunction<Tuple2<String,String>, String, String>() {

						@Override
						public Iterable<Tuple2<String, String>> call(
								Tuple2<String, String> file) throws Exception {
							List results = new ArrayList();
						      List<String> users = Arrays.asList(file._1().split(","));
						    	      String add = file._2();
						      for (String user : users) {
						        Tuple2<String, String> result = new Tuple2<>(user, add);
						        results.add(result);
						      }
							return results;
						}
					});
				 
				 
				 /**
				  * result:
				  * 288291234	"HUBSC97HWE_HUB3230德清乾元析家墩GT"    1405495468
					 284817762	"HURNC1.5379"    1405458554
				  * */
				 JavaPairRDD<String, Long> pairs1=pairs.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {

					@Override
					public Tuple2<String, Long> call(Tuple2<String, String> t)
							throws Exception {
						String add="";
						String user="";
						Long time=0L;
						add=t._2();
						String[]strs=t._1().split("\\|");
						user=strs[0];
						time=Long.parseLong(strs[1]);
						
						return new Tuple2<String, Long>(user+"	"+add,time);
					}
				});
				 
				 /**
				  * results:
				  * 
				  * 278346188	"HUBSC99HWE_HUB0193吴兴织里世诚服饰"    [1405500372, 1405500658, 1405500184, 1405510253, 1405472705, 1405500433, 1405487099, 1405509999, 1405494958]
					278013713	"HUBSC97HWE_HUB3129德清禹越"    [1405470001]
					259775566	"HURNC2.342"    [1405498406, 1405497848]
				  * */
				   JavaPairRDD<String, Iterable<Long>> result_user_Add_times = pairs1.groupByKey();
				
				
		     		/*for (Tuple2<String, String> tuple2 : pairs.distinct().collect()) {
		     			System.out.println(tuple2._1() + "    " + tuple2._2());
		     		}*/
				/* for (Tuple2<String, Long> tuple2 : pairs1.distinct().collect()) {
		     			System.out.println(tuple2._1() + "    " + tuple2._2());
		     		}*/
				/*for (Tuple2<String, Iterable<String>> tuple2 : result_user_Adds.distinct().collect()) {
	     			System.out.println(tuple2._1() + "    " + tuple2._2());
	     		}*/
				   
				   /*for (Tuple2<String, Iterable<Long>> tuple2 : result_user_Add_times.distinct().collect()) {
		     			System.out.println(tuple2._1() + "    " + tuple2._2());
		     		}*/
				   
				// 对分组后的数据进行KV对换
					JavaPairRDD<String, Long> timeall = result_user_Add_times//
							.mapToPair(new PairFunction<Tuple2<String, Iterable<Long>>, String, Long>() {
								@Override
								public Tuple2<String, Long> call(
										Tuple2<String, Iterable<Long>> userTuple2)
										throws Exception {
									Iterator<Long> list = userTuple2._2().iterator();
									String sp = "";
									while (list.hasNext()) {
										sp += list.next() + "|";
									}
									String[] time = sp.split("\\|");
									long times = 0;
									if (time.length>1) {
										for (int i = time.length - 1; i >= 0; i--) {
											if (i >=1&&i!=0) {
												if (Math.abs(Long.parseLong(time[i])- Long.parseLong(time[i - 1])) < IntervalTime * 60) {
													times += Math.abs(Long.parseLong(time[i])- Long.parseLong(time[i - 1]));
												}else {
													times+=0;
												}
											}
										}
									}
									
									return new Tuple2<String, Long>(userTuple2._1(), times);
								}
							});
					
					
					for (Tuple2<String, Long> tuple2 : timeall.distinct().collect()) {
		     			System.out.println(tuple2._1() + "    " + tuple2._2());
		     		}

		

	}
	


}
