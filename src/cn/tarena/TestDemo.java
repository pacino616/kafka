package cn.tarena;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;

public class TestDemo {
	
	/*
	 * 通过代码连接Kafka服务并生成数据
	 */
	@Test
	public void producer() throws  ExecutionException{
		Properties props=new Properties();
		//key的序列化类型，相当于offset，偏移量
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
	    //value的序列化类型，相当于写入的值
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    //kafka的服务节点
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.80.72:9092,192.168.80.73:9092");
	    //至少一次
//	    props.put("scks", "all");
	    //至多一次
//	    props.put("acks", "0");
	    //结合“acks”,"all"实现精确语义
//	    props.put("enable.idmpotence", "true");
	    
		Producer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(props);
		for(int i=0;i<100;i++){
			ProducerRecord<Integer, String> message 
			  = new ProducerRecord<Integer, String>("sbsb",""+i);
			kafkaProducer.send(message);
		}
		
		while(true);
	}
	
	@Test
	public void create_topic(){
		//zk的服务地址，写一个也可以，存在宕机的风险
		ZkUtils zkUtils = ZkUtils.apply(
				"192.168.80.72:2181,"
				+ "192.168.80.73:2181,"
				+ "192.168.80.74:2181", 
				30000, 30000, JaasUtils.isZkSecurityEnabled());
		// 创建一个单分区单副本名为t1的topic
		//--①参：zk连接参数 ②参:主题名  ③参:分区数  ④参:副本数
		AdminUtils.createTopic(zkUtils, "t2",3,2, new Properties(), RackAwareMode.Enforced$.MODULE$);
		zkUtils.close();
	}
	
	/*
	 * 删除主题
	 */
	@Test
	public void delete_topic(){
		ZkUtils zkUtils = ZkUtils.apply("192.168.80.72:2181,192.168.80.73:2181,192.168.80.74:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
		// 删除topic 't1'
		AdminUtils.deleteTopic(zkUtils, "t1");
		zkUtils.close();
	}
	
	/*
	 * 消费数据
	 */
	@Test
	public void consumer_1(){
		Properties props = new Properties();
		//--设置kafka的服务列表
		props.put("bootstrap.servers", "192.168.80.72:9092,192.168.80.73:9092");
		//--指定当前的消费者属于哪个消费者组 g1是自己定的
		props.put("group.id", "g1");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		//--指定消费者消费的主题，可以指定多个
		consumer.subscribe(Arrays.asList("enbook", "t2"));
		
		try {
			  while (true) {
				//--消费者从分区队列中消费数据，用到poll阻塞方法，如果没有数据可以消费，则一直阻塞
				//concurrentMap里面就用到了poll方法
			    ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
			    for (ConsumerRecord<String, String> record : records)
			    	
			      System.out.println("g1组c1消费者,分区编号:"+record.partition()+"offset:"+record.offset() + ":" + record.value());
			  }
			} catch (Exception e) {
			} finally {
			  consumer.close();
			}
	}
	
	
	@Test
	public void consumer_2(){
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.80.72:9092,192.168.80.73:9092");
		//--指定当前的消费者属于哪个消费者组 g1是自己定的
		props.put("group.id", "g1");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		//消费者收到消息后，0.1s会自动提交offset，这层语义是最多一次
//		props.put("enable.auto.commit", "true");
//		props.put("auto.commit.interval.ms", "101");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		//--指定消费者消费的主题，可以指定多个
		consumer.subscribe(Arrays.asList("sbsb", "t2"));
		
		try {
			  while (true) {
			    ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
			    for (ConsumerRecord<String, String> record : records)
			      System.out.println("g1组c2消费者,分区编号:"+record.partition()+"offset:"+record.offset() + ":" + record.value());
			  }
			} catch (Exception e) {
			} finally {
			  consumer.close();
			}
	}
	@Test
	public void testE(){
		
		int a=Math.abs("console-consumer-43455".hashCode())%50;
		System.out.println(a);

	}
	
	
	


}
