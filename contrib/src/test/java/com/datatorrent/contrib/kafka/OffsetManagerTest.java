package com.datatorrent.contrib.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.contrib.kafka.AbstractPartitionableKafkaInputOperator.PartitionStrategy;

public class OffsetManagerTest extends KafkaOperatorTestBase
{

  public OffsetManagerTest()
  {
    // This class want to initialize several kafka brokers for multiple partitions
    hasMultiPartition = true;
  }
  
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaPartitionableInputOperatorTest.class);
  static List<String> collectedTuples = new LinkedList<String>();
  static final int totalCount = 100;
  static CountDownLatch latch;

  
  public static class TestOffsetManager implements OffsetManager{

    private final Map<Integer, Long> offsets = new HashMap<Integer, Long>();
    
    @Override
    public Map<Integer, Long> loadInitialOffsets()
    {
      return offsets;
    }

    @Override
    public void updateOffsets(Map<Integer, Long> offsetsOfPartitions)
    {
      
      offsets.putAll(offsetsOfPartitions);
      
      if (latch.getCount() == 1) {
        // when latch is 1, it means consumer has consumed all the messages
        int count = 0;
        for (Entry<Integer, Long> entry : offsets.entrySet()) {
          count += entry.getValue();
        }
        if (count == totalCount + 2) {
          // wait until all offsets add up to totalCount messages + 2 control END_TUPLE
          latch.countDown();
        }
      }
    }
    
  }
  /**
   * Test Operator to collect tuples from KafkaSingleInputStringOperator.
   * 
   * @param <T>
   */
  public static class CollectorModule extends BaseOperator
  {
    public final transient CollectorInputPort inputPort = new CollectorInputPort(this);
  }

  public static class CollectorInputPort extends DefaultInputPort<String>
  {

    public CollectorInputPort(Operator module)
    {
      super();
    }

    @Override
    public void process(String tuple)
    {
      if (tuple.equals(KafkaOperatorTestBase.END_TUPLE)) {
        if (latch != null) {
          latch.countDown();
        }
        return;
      }
      collectedTuples.add(tuple);
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
        collectedTuples.clear();
      }
    }
  }
  
  /**
   * Test OffsetManager update offsets in Simple Consumer
   * 
   * [Generate send 100 messages to Kafka] ==> [wait until the offsets has been updated to 102 or timeout after 30s which means offset has not been updated]
   * 
   * 
   * @throws Exception
   */
  @Test
  public void testSimpleConsumerUpdateOffsets() throws Exception
  {
    // Create template simple consumer
    SimpleKafkaConsumer consumer = new SimpleKafkaConsumer();
    testPartitionableInputOperator(consumer);
  }
  
  public void testPartitionableInputOperator(KafkaConsumer consumer) throws Exception{
    
    // Set to 3 because we want to make sure END_TUPLE from both 2 partitions are received and offsets has been updated to 102
    latch = new CountDownLatch(3);
    
    // Start producer
    KafkaTestProducer p = new KafkaTestProducer(TEST_TOPIC, true);
    p.setProducerType("sync");
    p.setSendCount(totalCount);
    // wait the producer send all messages
    p.run();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    // Create KafkaSinglePortStringInputOperator
    PartitionableKafkaSinglePortStringInputOperator node = dag.addOperator("Kafka message consumer", PartitionableKafkaSinglePortStringInputOperator.class);
    
    node.setOffsetManager(new TestOffsetManager());
    
    node.setStrategy(PartitionStrategy.ONE_TO_MANY.toString());
    
    //set topic
    consumer.setTopic(TEST_TOPIC);
    //set the brokerlist used to initialize the partition
    Set<String> brokerSet =  new HashSet<String>();
    brokerSet.add("localhost:9092");
    brokerSet.add("localhost:9093");
    consumer.setBrokerSet(brokerSet);
    consumer.setInitialOffset("earliest");

    node.setConsumer(consumer);
    
    // Set the partition
    dag.setAttribute(node, OperatorContext.INITIAL_PARTITION_COUNT, 1);

    // Create Test tuple collector
    CollectorModule collector = dag.addOperator("TestMessageCollector", new CollectorModule());

    // Connect ports
    dag.addStream("Kafka message", node.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(true);

    lc.runAsync();
    
    // Wait 30s for consumer finish consuming all the messages and offsets has been updated to 100
    assertTrue("TIMEOUT: 30s ", latch.await(30000, TimeUnit.MILLISECONDS));
    
    
    // Check results
    assertEquals("Tuple count", totalCount, collectedTuples.size());
    logger.debug(String.format("Number of emitted tuples: %d", collectedTuples.size()));
    
    p.close();
    lc.shutdown();
  }

}
