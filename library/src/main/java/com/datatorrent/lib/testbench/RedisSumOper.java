/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.testbench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;

/**
 * <p>RedisSumOper class.</p>
 *
 * @since 0.3.2
 */
public class RedisSumOper extends BaseOperator
{
	private ArrayList<Integer> collect;
	public final transient DefaultInputPort<Integer> inport = new DefaultInputPort<Integer>() {
	    @Override
	    public void process(Integer s) {
	      collect.add(s);
	    }
	};

	@Override
	public void setup(OperatorContext context)
	{
	}

	@Override
	public void teardown()
	{
	}

	@Override
	public void beginWindow(long windowId)
	{
		collect  = new ArrayList<Integer>();
	}
	
	// out port
	public final transient DefaultOutputPort<Map<Integer, Integer>> outport = new DefaultOutputPort<Map<Integer, Integer>>();
	
	@Override
	public void endWindow()
	{
		Integer sum = 0;
		for(Integer entry : collect) sum += entry;
		Map<Integer, Integer> tuple = new HashMap<Integer, Integer>();
		tuple.put(1, sum);
		outport.emit(tuple);
	}
}
