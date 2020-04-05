/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Internal;

/**
 * 使用作为一个虚拟的KeySelector用于的keyed算子对于non-keyed使用用例。
 * Used as a dummy {@link KeySelector} to allow using keyed operators
 * for non-keyed use cases.
 * 本质上，它给全部传入记录一个相同的key，这是{@code（byte）0}的值
 * Essentially, it gives all incoming records
 * the same key, which is a {@code (byte) 0} value.
 *
 * @param <T> The type of the input element.
 */
@Internal
public class NullByteKeySelector<T> implements KeySelector<T, Byte> {

	private static final long serialVersionUID = 614256539098549020L;

	@Override
	public Byte getKey(T value) throws Exception {
		return 0;
	}
}
