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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * 使用锁定引用计数器为传入事件和 {@link SharedBufferNode}实现锁定逻辑。
 * Implements locking logic for incoming event and
 * {@link SharedBufferNode} using a lock reference counter.
 */
public final class Lockable<T> {

	//引用计数器
	private int refCounter;

	//元素
	private final T element;

	public Lockable(T element, int refCounter) {
		this.refCounter = refCounter;
		this.element = element;
	}

	//加锁引用计数器+1
	public void lock() {
		refCounter += 1;
	}

	/**
	 * Releases lock on this object. If no more locks are acquired on it, this method will return true.
	 * 在这个对象释放锁，如果没有其他锁，此方法将返回true。
	 * @return true if no more locks are acquired
	 */
	boolean release() {
		//如果这个对象没有其他锁
		if (refCounter <= 0) {
			return true;
		}
		//锁减1
		refCounter -= 1;
		//是否还有锁
		return refCounter == 0;
	}

	public T getElement() {
		return element;
	}

	@Override
	public String toString() {
		return "Lock{" +
			"refCounter=" + refCounter +
			'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Lockable<?> lockable = (Lockable<?>) o;
		return refCounter == lockable.refCounter &&
			Objects.equals(element, lockable.element);
	}

	@Override
	public int hashCode() {
		return Objects.hash(refCounter, element);
	}

	/** Serializer for {@link Lockable}. */
	public static class LockableTypeSerializer<E> extends TypeSerializer<Lockable<E>> {
		private static final long serialVersionUID = 3298801058463337340L;
		private final TypeSerializer<E> elementSerializer;

		LockableTypeSerializer(TypeSerializer<E> elementSerializer) {
			this.elementSerializer = elementSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<Lockable<E>> duplicate() {
			return new LockableTypeSerializer<>(elementSerializer);
		}

		@Override
		public Lockable<E> createInstance() {
			return null;
		}

		@Override
		public Lockable<E> copy(Lockable<E> from) {
			return new Lockable<E>(elementSerializer.copy(from.element), from.refCounter);
		}

		@Override
		public Lockable<E> copy(
			Lockable<E> from, Lockable<E> reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(Lockable<E> record, DataOutputView target) throws IOException {
			IntSerializer.INSTANCE.serialize(record.refCounter, target);
			elementSerializer.serialize(record.element, target);
		}

		@Override
		public Lockable<E> deserialize(DataInputView source) throws IOException {
			int refCount = IntSerializer.INSTANCE.deserialize(source);
			E record = elementSerializer.deserialize(source);
			return new Lockable<>(record, refCount);
		}

		@Override
		public Lockable<E> deserialize(
			Lockable<E> reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			IntSerializer.INSTANCE.copy(source, target); // refCounter

			E element = elementSerializer.deserialize(source);
			elementSerializer.serialize(element, target);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			LockableTypeSerializer<?> that = (LockableTypeSerializer<?>) o;
			return Objects.equals(elementSerializer, that.elementSerializer);
		}

		@Override
		public int hashCode() {
			return Objects.hash(elementSerializer);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj.getClass().equals(LockableTypeSerializer.class);
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return elementSerializer.snapshotConfiguration();
		}

		@Override
		public CompatibilityResult<Lockable<E>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			CompatibilityResult<E> inputComaptibilityResult = elementSerializer.ensureCompatibility(configSnapshot);
			if (inputComaptibilityResult.isRequiresMigration()) {
				return CompatibilityResult.requiresMigration(new LockableTypeSerializer<>(
					new TypeDeserializerAdapter<>(inputComaptibilityResult.getConvertDeserializer()))
				);
			} else {
				return CompatibilityResult.compatible();
			}
		}
	}
}
