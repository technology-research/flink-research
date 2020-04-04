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

import org.apache.flink.util.Preconditions;

import java.util.EnumSet;
import java.util.Objects;

/**
 * 描述模式的量词。 {@link Quantifier}主要分为三类。
 * A quantifier describing the Pattern. There are three main groups of {@link Quantifier}.
 *
 * <p><ol>
 *     <li>Single</li> 单例
 *     <li>Looping</li> 循环
 *     <li>Times</li> 次数
 * </ol>
 *
 * 每个Pattern都是可选的，并且具有{@link ConsumingStrategy}。
 * Looping 和 Times还具有额外的内部消耗策略，该策略在模式中的已接受事件之间应用。
 * <p>Each {@link Pattern} can be optional and have a {@link ConsumingStrategy}. Looping and Times also hava an
 * additional inner consuming strategy that is applied between accepted events in the pattern.
 */
public class Quantifier {

	//量词属性
	private final EnumSet<QuantifierProperty> properties;

	//消费策略
	private final ConsumingStrategy consumingStrategy;

	//内嵌消费策略 跳至下一个
	private ConsumingStrategy innerConsumingStrategy = ConsumingStrategy.SKIP_TILL_NEXT;

	private Quantifier(
			final ConsumingStrategy consumingStrategy,
			final QuantifierProperty first,
			final QuantifierProperty... rest) {
		this.properties = EnumSet.of(first, rest);
		this.consumingStrategy = consumingStrategy;
	}

	//至少一次
	public static Quantifier one(final ConsumingStrategy consumingStrategy) {
		//使用量词属性为单例 事件执行一次
		return new Quantifier(consumingStrategy, QuantifierProperty.SINGLE);
	}

	//循环 测试属性为循环
	public static Quantifier looping(final ConsumingStrategy consumingStrategy) {
		return new Quantifier(consumingStrategy, QuantifierProperty.LOOPING);
	}

	//次数
	public static Quantifier times(final ConsumingStrategy consumingStrategy) {
		return new Quantifier(consumingStrategy, QuantifierProperty.TIMES);
	}

	//是否包含该属性
	public boolean hasProperty(QuantifierProperty property) {
		return properties.contains(property);
	}

	public ConsumingStrategy getInnerConsumingStrategy() {
		return innerConsumingStrategy;
	}

	public ConsumingStrategy getConsumingStrategy() {
		return consumingStrategy;
	}

	//校验模式
	private static void checkPattern(boolean condition, Object errorMessage) {
		if (!condition) {
			throw new MalformedPatternException(String.valueOf(errorMessage));
		}
	}

	//组合校验模式
	public void combinations() {
		checkPattern(!hasProperty(QuantifierProperty.SINGLE), "Combinations not applicable to " + this + "!");
		checkPattern(innerConsumingStrategy != ConsumingStrategy.STRICT, "You can apply apply either combinations or consecutive, not both!");
		checkPattern(innerConsumingStrategy != ConsumingStrategy.SKIP_TILL_ANY, "Combinations already applied!");

		innerConsumingStrategy = ConsumingStrategy.SKIP_TILL_ANY;
	}

	//循环模式调用组合为严格临近
	public void consecutive() {
		//循环模式或者测试模式
		checkPattern(hasProperty(QuantifierProperty.LOOPING) || hasProperty(QuantifierProperty.TIMES), "Combinations not applicable to " + this + "!");
		//内嵌测试部位SKIP_TILL_ANY或STRICT
		checkPattern(innerConsumingStrategy != ConsumingStrategy.SKIP_TILL_ANY, "You can apply apply either combinations or consecutive, not both!");
		checkPattern(innerConsumingStrategy != ConsumingStrategy.STRICT, "Combinations already applied!");

		innerConsumingStrategy = ConsumingStrategy.STRICT;
	}

	//可选模式
	public void optional() {
		//OPTIONAL属性不存在
		checkPattern(!hasProperty(QuantifierProperty.OPTIONAL), "Optional already applied!");
		//消费策略不能为NOT_NEXT  NOT_FOLLOW
		checkPattern(!(consumingStrategy == ConsumingStrategy.NOT_NEXT ||
					consumingStrategy == ConsumingStrategy.NOT_FOLLOW), "NOT pattern cannot be optional");

		//添加属性
		properties.add(Quantifier.QuantifierProperty.OPTIONAL);
	}

	//贪婪测试
	public void greedy() {
		//内嵌模式不能为跳过任意事件
		checkPattern(!(innerConsumingStrategy == ConsumingStrategy.SKIP_TILL_ANY),
			"Option not applicable to FollowedByAny pattern");
		//不能饮用在单一事件上
		checkPattern(!hasProperty(Quantifier.QuantifierProperty.SINGLE),
			"Option not applicable to singleton quantifier");

		properties.add(QuantifierProperty.GREEDY);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Quantifier that = (Quantifier) o;
		return Objects.equals(properties, that.properties) &&
				consumingStrategy == that.consumingStrategy &&
				innerConsumingStrategy == that.innerConsumingStrategy;
	}

	@Override
	public int hashCode() {
		return Objects.hash(properties, consumingStrategy, innerConsumingStrategy);
	}

	@Override
	public String toString() {
		return "Quantifier{" +
			"properties=" + properties +
			", consumingStrategy=" + consumingStrategy +
			", innerConsumingStrategy=" + innerConsumingStrategy +
			'}';
	}

	/**
	 * Properties that a {@link Quantifier} can have. Not all combinations are valid.
	 */
	public enum QuantifierProperty {
		//单例
		SINGLE,
		//循环
		LOOPING,
		//次数 times
		TIMES,
		//可选
		OPTIONAL,
		//贪婪 尽可能的重复
		GREEDY
	}

	/**
	 * 描述在此Pattern中匹配事件的策略。请参阅文档以获取更多信息。
	 * Describes strategy for which events are matched in this {@link Pattern}. See docs for more info.
	 * 消费策略
	 */
	public enum ConsumingStrategy {
		//严格模式
		STRICT,
		//移至下一个
		SKIP_TILL_NEXT,
		//移至任意
		SKIP_TILL_ANY,

		//不关注
		NOT_FOLLOW,
		//不下一个
		NOT_NEXT
	}

	/**
	 * 描述测试在Pattern能够使用
	 * Describe the times this {@link Pattern} can occur.
	 */
	public static class Times {
		private final int from;
		private final int to;

		private Times(int from, int to) {
			Preconditions.checkArgument(from > 0, "The from should be a positive number greater than 0.");
			Preconditions.checkArgument(to >= from, "The to should be a number greater than or equal to from: " + from + ".");
			this.from = from;
			this.to = to;
		}

		public int getFrom() {
			return from;
		}

		public int getTo() {
			return to;
		}

		public static Times of(int from, int to) {
			return new Times(from, to);
		}

		public static Times of(int times) {
			return new Times(times, times);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Times times = (Times) o;
			return from == times.from &&
				to == times.to;
		}

		@Override
		public int hashCode() {
			return Objects.hash(from, to);
		}
	}
}
