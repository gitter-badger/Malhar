/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.TestSink;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MarginTest
{
  private static Logger LOG = LoggerFactory.getLogger(Margin.class);


  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new Margin<String, Integer>());
  }

  public void testNodeProcessingSchema(Margin oper)
  {
    TestSink marginSink = new TestSink();

    oper.margin.setSink(marginSink);
    oper.setup(new OperatorConfiguration());

    oper.beginWindow();
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 2);
    input.put("b", 20);
    input.put("c", 1000);
    oper.numerator.process(input);

    input.clear();
    input.put("a", 2);
    input.put("b", 40);
    input.put("c", 500);
    oper.denominator.process(input);

    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", 1, marginSink.collectedTuples.size());

    for (Object o: marginSink.collectedTuples) {
      HashMap<String, Number> output = (HashMap<String, Number>)o;
      for (Map.Entry<String, Number> e: output.entrySet()) {
        LOG.debug(String.format("Key, value is %s,%f", e.getKey(), e.getValue().doubleValue()));
        if (e.getKey().equals("a")) {
          Assert.assertEquals("emitted value for 'a' was ", new Double(0), e.getValue());
        }
        else if (e.getKey().equals("b")) {
          Assert.assertEquals("emitted tuple for 'b' was ", new Double(0.5), e.getValue());
        }
        else if (e.getKey().equals("c")) {
          Assert.assertEquals("emitted tuple for 'c' was ", new Double(-1.0), e.getValue());
        }
        else {
          LOG.debug(String.format("key was %s", e.getKey()));
        }
      }
    }
  }
}