/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * A stream source function that returns a sequence of elements.
 * 一个流数据源函数返回元素的序列
 *
 * <p>Upon construction, this source function serializes the elements using Flink's type information.
 * That way, any object transport using Java serialization will not be affected by the serializability
 * of the elements.</p>
 * 构建后，此数据源函数使用Flink的类型信息对元素进行序列化。这样，使用Java序列化的任何对象传输都不会受到元素的可序列化性的影响。
 *
 * <p><b>NOTE:</b> This source has a parallelism of 1.这是并行度为1的数据源
 *
 * @param <T> The type of elements returned by this function.
 *            元素的类型是这个函数的返回值
 */
@PublicEvolving
public class FromElementsFunction<T> implements SourceFunction<T>, CheckpointedFunction {

	private static final long serialVersionUID = 1L;

	/**
	 * The (de)serializer to be used for the data elements.
	 * 这个（反）序列号器将用于数据元素
	 */
	private final TypeSerializer<T> serializer;

	/**
	 * The actual data elements, in serialized form.
	 * 真正的数据元素，以序列化形式
	 **/
	private final byte[] elementsSerialized;

	/**
	 * The number of serialized elements.
	 * 序列号元素的个数
	 **/
	private final int numElements;

	/**
	 * The number of elements emitted already.
	 * 已经输出元素的个数
	 **/
	private volatile int numElementsEmitted;

	/**
	 * The number of elements to skip initially.
	 * 初始化需要跳过的元素个数
	 **/
	private volatile int numElementsToSkip;

	/**
	 * Flag to make the source cancelable.
	 * 数据源取消标志
	 **/
	private volatile boolean isRunning = true;

	//ListState存储的状态
	private transient ListState<Integer> checkpointedState;

	public FromElementsFunction(TypeSerializer<T> serializer, T... elements) throws IOException {
		this(serializer, Arrays.asList(elements));
	}

	public FromElementsFunction(TypeSerializer<T> serializer, Iterable<T> elements) throws IOException {
		//得到字节输出流
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		//将OutputStream转换为DataOutputView
		DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);

		//序列化元素个数
		int count = 0;
		try {
			for (T element : elements) {
				//序列化元素
				serializer.serialize(element, wrapper);
				count++;
			}
		} catch (Exception e) {
			throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
		}

		this.serializer = serializer;
		this.elementsSerialized = baos.toByteArray();
		//复制元素个数
		this.numElements = count;
	}

	/**
	 * 初始化状态
	 *
	 * @param context the context for initializing the operator
	 * @throws Exception
	 */
	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		//如果checkpointedState不为空，代表已经初始化过了
		Preconditions.checkState(this.checkpointedState == null,
			"The " + getClass().getSimpleName() + " has already been initialized.");

		//Operator State 从上下文中得到listState，然后创建一个新的ListState修饰符，并且声明一个Int序列化器实例
		this.checkpointedState = context.getOperatorStateStore().getListState(
			new ListStateDescriptor<>(
				"from-elements-state",
				IntSerializer.INSTANCE
			)
		);

		//如果是stateful任务，状态从上一个执行快照中获取
		if (context.isRestored()) {
			//取回状态值列表
			List<Integer> retrievedStates = new ArrayList<>();
			//从checkpoint状态列表中获取state，注意这个state只是listState内部的state值
			for (Integer entry : this.checkpointedState.get()) {
				retrievedStates.add(entry);
			}

			// given that the parallelism of the function is 1, we can only have 1 state
			//如果并行度不为1抛出异常
			Preconditions.checkArgument(retrievedStates.size() == 1,
				getClass().getSimpleName() + " retrieved invalid state.");
			//跳过元素个数
			this.numElementsToSkip = retrievedStates.get(0);
		}
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
		final DataInputView input = new DataInputViewStreamWrapper(bais);

		// if we are restored from a checkpoint and need to skip elements, skip them now.
		//如果我们已从检查点恢复并且需要跳过元素，请立即跳过它们。
		int toSkip = numElementsToSkip;
		if (toSkip > 0) {
			try {
				while (toSkip > 0) {
					//反序列化该输入数据
					serializer.deserialize(input);
					toSkip--;
				}
			} catch (Exception e) {
				throw new IOException("Failed to deserialize an element from the source. " +
					"If you are using user-defined serialization (Value and Writable types), check the " +
					"serialization functions.\nSerializer is " + serializer);
			}
			//输出元素个数
			this.numElementsEmitted = this.numElementsToSkip;
		}
		//拿到检查点锁
		final Object lock = ctx.getCheckpointLock();
		//如果输出元素小于元素个数，并且是运行状态
		while (isRunning && numElementsEmitted < numElements) {
			T next;
			try {
				//继续反序列化元素
				next = serializer.deserialize(input);
			} catch (Exception e) {
				throw new IOException("Failed to deserialize an element from the source. " +
					"If you are using user-defined serialization (Value and Writable types), check the " +
					"serialization functions.\nSerializer is " + serializer);
			}
			//顺序方式输出input元素
			synchronized (lock) {
				ctx.collect(next);
				numElementsEmitted++;
			}
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}


	/**
	 * Gets the number of elements produced in total by this function.
	 *
	 * @return The number of elements produced in total.
	 */
	public int getNumElements() {
		return numElements;
	}

	/**
	 * Gets the number of elements emitted so far.
	 *
	 * @return The number of elements emitted so far.
	 */
	public int getNumElementsEmitted() {
		return numElementsEmitted;
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------
	//快照存储元素放入liststate中
	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkState(this.checkpointedState != null,
			"The " + getClass().getSimpleName() + " has not been properly initialized.");
		//更新快照状态，先清空，在添加输出元素到liststate中
		this.checkpointedState.clear();
		this.checkpointedState.add(this.numElementsEmitted);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Verifies that all elements in the collection are non-null, and are of the given class, or
	 * a subclass thereof.
	 *
	 * @param elements The collection to check.
	 * @param viewedAs The class to which the elements must be assignable to.
	 * @param <OUT>    The generic type of the collection to be checked.
	 */
	public static <OUT> void checkCollection(Collection<OUT> elements, Class<OUT> viewedAs) {
		for (OUT elem : elements) {
			if (elem == null) {
				throw new IllegalArgumentException("The collection contains a null element");
			}

			if (!viewedAs.isAssignableFrom(elem.getClass())) {
				throw new IllegalArgumentException("The elements in the collection are not all subclasses of " +
					viewedAs.getCanonicalName());
			}
		}
	}
}
