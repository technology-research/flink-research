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

package org.apache.flink.cep.pattern;

import org.apache.flink.cep.nfa.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

/**
 * Base class for a group pattern definition.
 * 定义一组模式的基础类
 * @param <T> Base type of the elements appearing in the pattern  模式中出现的元素的基本类型
 * @param <F> Subtype of T to which the current pattern operator is constrained 自类型
 */
public class GroupPattern<T, F extends T> extends Pattern<T, F> {

	/** Group pattern representing the pattern definition of this group. */
	//组模式代表这个组的模式定义
	private final Pattern<T, ? extends T> groupPattern;

	/**
	 *
	 * @param previous 上一个模式
	 * @param groupPattern 组模式
	 * @param consumingStrategy 消费测试
	 * @param afterMatchSkipStrategy 匹配之后跳过测试
	 */
	GroupPattern(
		final Pattern<T, ? extends T> previous,
		final Pattern<T, ? extends T> groupPattern,
		final Quantifier.ConsumingStrategy consumingStrategy,
		final AfterMatchSkipStrategy afterMatchSkipStrategy) {
		super("GroupPattern", previous, consumingStrategy, afterMatchSkipStrategy);
		this.groupPattern = groupPattern;
	}

	@Override
	public Pattern<T, F> where(IterativeCondition<F> condition) {
		throw new UnsupportedOperationException("GroupPattern does not support where clause.");
	}

	@Override
	public Pattern<T, F> or(IterativeCondition<F> condition) {
		throw new UnsupportedOperationException("GroupPattern does not support or clause.");
	}

	@Override
	public <S extends F> Pattern<T, S> subtype(final Class<S> subtypeClass) {
		throw new UnsupportedOperationException("GroupPattern does not support subtype clause.");
	}

	//拿到组模式
	public Pattern<T, ? extends T> getRawPattern() {
		return groupPattern;
	}
}
