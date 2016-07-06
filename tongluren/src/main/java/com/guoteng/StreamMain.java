package com.guoteng;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class StreamMain  {
	//topic名
	public final static String TOPIC = "TEST-TOPIC";
	//分区名
	int messageNo = 0;
	//自增量
	static double ic=0;
	//声明一个生产者
	Producer<String, String> producer = new Producer<String, String>(
			new ProducerConfig(getProperties()));
	class Myhandler extends ChannelInboundHandlerAdapter {
		public void channelRead(ChannelHandlerContext ctx, Object msg)
				throws Exception {
			System.out.println("333");
			String response = msg.toString();
			System.out.println(response);
			//通过kafka发送消息
			String key = String.valueOf(messageNo);
			//消息
			String data = response;
			//发送消息
			producer.send(new KeyedMessage<String, String>(TOPIC, key, data));
			//把String类型的消息转换为ByteBuf
			ByteBuf encord = ctx.alloc().buffer(4 * response.length());
			encord.writeBytes(response.getBytes());
			//逐字地把接受到的消息写入缓冲区
			ctx.write(encord);
		}
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
				throws Exception {
			// TODO Auto-generated method stub
			cause.printStackTrace();
			ctx.close();
		}
	}
	//把kafka的配置封装成一个方法
	private static Properties getProperties() {
		Properties props = new Properties();
		// 此处配置的是kafka的端口
		props.put("metadata.broker.list", "localhost:9092");
		// 配置value的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 配置key的序列化类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "-1");
		return props;
	}
	
	public static String[] getDvalue(String[] v1,String[] v2){
		String result[]=new String[v1.length];
		DecimalFormat df = new DecimalFormat("##0.0000");  
		for(int i=0;i<v1.length;i++){
			double r1=Double.parseDouble(v2[i])-Double.parseDouble(v1[i]);
			result[i]=df.format(r1);
		}
		return result;	
	}
	public static String[] getEquvalue(String[] v1,String v2){
		String result[]=new String[v1.length];
		DecimalFormat df = new DecimalFormat("##0.0000");  
		for(int i=0;i<v1.length;i++){
			double r1=Double.parseDouble(v2)*Double.parseDouble(v1[i]);
			result[i]=df.format(r1);
		}
		return result;	
	}
	public static String getStringFromArray(String[] v){
		return v[0]+", "+v[1]+", "+v[2]+", "+v[3]+", "+v[4]+", "+v[5]+", "+
				v[6]+", "+v[7]+", "+v[8]+", "+v[9]+", "+v[10]+", "+v[11];
	}
	
	public static String arrayToString(String[] value){
		StringBuffer sb=new StringBuffer();
		for(int i=0;i<value.length;i++){
			sb.append(value[i]);
		}
		return sb.toString();
	}
	public static String  getIncrement(){
		DecimalFormat df = new DecimalFormat("000000000.#");  
		ic++;
		return df.format(ic);
	}

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {
		final String tableName = "text";
		final String columnFamily = "data";
		final String table="t_ec_energyitemresult";
		final SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final List<String> list=new ArrayList<String>();
		list.add(columnFamily);
		final HbaseTable hTable=new HbaseTable();
		MysqlToSpark sqlTospark=new MysqlToSpark();
		JavaSparkContext jsc=new JavaSparkContext("local[2]",
				"MyReceiverAndHandler");
		final JavaPairRDD<String, String> sqlRdd=sqlTospark.getMysqlRDD(jsc,"t_dt_energyitemdict");
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(5000));

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TOPIC, new Integer(1));
		//使用spark streaming的自带接口接收kafka发送过来的消息
		JavaPairDStream<String, String> messages = KafkaUtils.createStream(
				jssc, "localhost", "jd-group", topicCountMap);

		// 获取消息
				JavaDStream<Entity> lines = messages
						.map(new Function<Tuple2<String, String>, Entity>() {

							@Override
							public Entity call(Tuple2<String, String> v1)
									throws Exception {
								// TODO Auto-generated method stub
								return Parse.parseJsonToRoot(v1._2());
							}
						});
				//把消息转换为存入hbase的格式
				JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Entity, String>() {

					@Override
					public Iterable<String> call(Entity t) throws Exception {
						// TODO Auto-generated method stub
						String coding=t.getRoot().getData().get(0).getMeter().get(0)
								.getFunctioncoding().get(0);
						String regin=coding.substring(10, 12);
						String time=String.valueOf(9999999999L-Long.parseLong(t.getRoot().getData().get(0)
								.getTime().get(0)));
						String tempVlaue1=t.getRoot().getData().get(0).getMeter().get(0)
								.getFunction().toString();
						String tempValue2=t.getRoot().getData().get(0).getMeter().get(1)
								.getFunction().toString();
						String value1=tempVlaue1.substring(1,tempVlaue1.indexOf("]"));
						String value2=tempValue2.substring(1,tempValue2.indexOf("]"));
						String result=regin+time+coding+":"+value1+", "+value2;
						List list=new ArrayList();
						list.add(result);
						return list;
					}
				});
		// 把信息换为（key,value）模式方便后面的操作
		JavaPairDStream<String, String> pairs = words
				.mapToPair(new PairFunction<String, String, String>() {
					public Tuple2<String, String> call(String s) {
						String result[]=s.split(":");
						String key=result[0];
						String value=result[1];
						return new Tuple2<String, String>(key,value);
					}
				});
		//将数据存入hbase 
		pairs.foreach(new Function2<JavaPairRDD<String, String>, Time, Void>() {

			public Void call(JavaPairRDD<String, String> v1, Time v2)
					throws Exception {
				// TODO Auto-generated method stub
				v1.foreach(new VoidFunction<Tuple2<String, String>>() {
					@Override
					public void call(Tuple2<String, String> tuple)
							throws Exception {
						// TODO Auto-generated method stub
						hTable.creat(tableName, list);
						hTable.put(tableName, tuple._1(),columnFamily, "value", tuple._2());
					}
				});
				return null;
			}
		});
		//得到新的key,value对
		JavaPairDStream<String, String> countes = words
				.mapToPair(new PairFunction<String, String, String>() {
					public Tuple2<String, String> call(String s) {
						String result[]=s.split(":");
						String key=result[0].substring(12);
						String time=String.valueOf(9999999999L-Long.parseLong(result[0].substring(2, 12)));
						String value=result[1]+";"+time;
						return new Tuple2<String, String>(key,value);
					}
				});
		countes.print();
		//计算15分钟变化量的算法
		JavaPairDStream<String, String> window=countes.reduceByKeyAndWindow(
				new Function2<String, String, String>() {
			
			@Override
			public String call(String v1, String v2) throws Exception {
				// TODO Auto-generated method stub
				String temp1[]=v1.split(";");
				String temp2[]=v2.split(";");
				System.out.println("temp1length:"+temp1.length+" "+arrayToString(temp1));
				System.out.println("temp2length:"+temp2.length+" "+arrayToString(temp2));
				String value1[]=temp1[0].split(", ");
				String value2[]=temp2[0].split(", ");
				String result1[]=StreamMain.getDvalue(value1, value2);
				System.out.println("length"+result1.length);
				return StreamMain.getStringFromArray(result1)+";"+temp1[1]+";"+temp2[1];
			}
		},new Duration(10000), new Duration(5000));
		//获得与分类分项表对应的key.value
		JavaPairDStream<String, String> cord=window.mapToPair(new PairFunction<Tuple2<String,String>, String,String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t)
					throws Exception {
				// TODO Auto-generated method stub
				String key=t._1().substring(10);
				String value=t._2()+";"+t._1().substring(0,10);
				return new Tuple2<String, String>(key, value);
			}
		});
		//计算转化标准煤后的数值
		JavaPairDStream<String, String> transf=cord.transformToPair(
				new Function<JavaPairRDD<String,String>, JavaPairRDD<String,String>>() {

					@Override
					public JavaPairRDD<String, String> call(
							JavaPairRDD<String, String> v1) throws Exception {
						// TODO Auto-generated method stub
						JavaPairRDD<String, Tuple2<String, String>> temp=v1.join(sqlRdd);
						JavaPairRDD<String, String> result=temp.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,String>>, String, String>() {

							@Override
							public Tuple2<String, String> call(
									Tuple2<String, Tuple2<String, String>> t)
									throws Exception {
								// TODO Auto-generated method stub
								String temp[]=t._2()._1().split(";");
								String value1[]=temp[0].split(", ");
								String result1[]=StreamMain.getEquvalue(value1, t._2()._2());
								return new Tuple2<String, String>(t._1(), t._2()._1()+";"
										+StreamMain.getStringFromArray(result1));
							}
						});
						return result;
					}
		});
		//打印最后的结果
		transf.print();
		//写入mysql
		transf.foreachRDD(new Function<JavaPairRDD<String,String>, Void>() {
			public Void call(JavaPairRDD<String, String> v1) throws Exception {
				// TODO Auto-generated method stub
				v1.foreachPartition(new VoidFunction<Iterator<Tuple2<String,String>>>() {
					
					@Override
					public void call(Iterator<Tuple2<String, String>> t) throws Exception {
						// TODO Auto-generated method stub
						String url = "jdbc:mysql://localhost:3306/test";
						String user = "root";
						String password ="123456";
						Class.forName("com.mysql.jdbc.Driver");
						Connection conn=DriverManager.getConnection(url, user, password);
						String sql="insert into "+table+" values(?,?,?,?,?,?,?,?)";
						PreparedStatement stmt=conn.prepareStatement(sql);
						while(t.hasNext()){
							Tuple2<String, String>tuple=t.next();
							String result[]=tuple._2().split(";");
							String value[]=result[0].split(", ");
							String EValue[]=result[4].split(", ");
							for(int i=0;i<value.length;i++){
								stmt.setString(1, result[3]+"1"+StreamMain.getIncrement());
								stmt.setString(2, result[3]);
								stmt.setString(3, tuple._1());
								Date d1=new Date(Long.parseLong(result[1])*1000);
								Date d2=new Date(Long.parseLong(result[2])*1000);
								stmt.setString(4, sdf.format(d1));
								stmt.setString(5, sdf.format(d2));
								stmt.setString(6, value[i]);
								stmt.setString(7, EValue[i]);
								stmt.setInt(8, 1);
								stmt.executeUpdate();
							}
						}
						stmt.close();
						conn.close();
					}
				});

				return null;
			}
		});
		//开始计算
		jssc.start();
		//等待计算终止
		jssc.awaitTermination();
	}
}
