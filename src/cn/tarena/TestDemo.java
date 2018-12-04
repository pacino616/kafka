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
	 * ͨ����������Kafka������������
	 */
	@Test
	public void producer() throws  ExecutionException{
		Properties props=new Properties();
		//key�����л����ͣ��൱��offset��ƫ����
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
	    //value�����л����ͣ��൱��д���ֵ
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    //kafka�ķ���ڵ�
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.80.72:9092,192.168.80.73:9092");
	    //����һ��
//	    props.put("scks", "all");
	    //����һ��
//	    props.put("acks", "0");
	    //��ϡ�acks��,"all"ʵ�־�ȷ����
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
		//zk�ķ����ַ��дһ��Ҳ���ԣ�����崻��ķ���
		ZkUtils zkUtils = ZkUtils.apply(
				"192.168.80.72:2181,"
				+ "192.168.80.73:2181,"
				+ "192.168.80.74:2181", 
				30000, 30000, JaasUtils.isZkSecurityEnabled());
		// ����һ����������������Ϊt1��topic
		//--�ٲΣ�zk���Ӳ��� �ڲ�:������  �۲�:������  �ܲ�:������
		AdminUtils.createTopic(zkUtils, "t2",3,2, new Properties(), RackAwareMode.Enforced$.MODULE$);
		zkUtils.close();
	}
	
	/*
	 * ɾ������
	 */
	@Test
	public void delete_topic(){
		ZkUtils zkUtils = ZkUtils.apply("192.168.80.72:2181,192.168.80.73:2181,192.168.80.74:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
		// ɾ��topic 't1'
		AdminUtils.deleteTopic(zkUtils, "t1");
		zkUtils.close();
	}
	
	/*
	 * ��������
	 */
	@Test
	public void consumer_1(){
		Properties props = new Properties();
		//--����kafka�ķ����б�
		props.put("bootstrap.servers", "192.168.80.72:9092,192.168.80.73:9092");
		//--ָ����ǰ�������������ĸ��������� g1���Լ�����
		props.put("group.id", "g1");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		//--ָ�����������ѵ����⣬����ָ�����
		consumer.subscribe(Arrays.asList("enbook", "t2"));
		
		try {
			  while (true) {
				//--�����ߴӷ����������������ݣ��õ�poll�������������û�����ݿ������ѣ���һֱ����
				//concurrentMap������õ���poll����
			    ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
			    for (ConsumerRecord<String, String> record : records)
			    	
			      System.out.println("g1��c1������,�������:"+record.partition()+"offset:"+record.offset() + ":" + record.value());
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
		//--ָ����ǰ�������������ĸ��������� g1���Լ�����
		props.put("group.id", "g1");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		//�������յ���Ϣ��0.1s���Զ��ύoffset��������������һ��
//		props.put("enable.auto.commit", "true");
//		props.put("auto.commit.interval.ms", "101");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		//--ָ�����������ѵ����⣬����ָ�����
		consumer.subscribe(Arrays.asList("sbsb", "t2"));
		
		try {
			  while (true) {
			    ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
			    for (ConsumerRecord<String, String> record : records)
			      System.out.println("g1��c2������,�������:"+record.partition()+"offset:"+record.offset() + ":" + record.value());
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
