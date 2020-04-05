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

package org.apache.flink.annotation.docs;

import org.apache.flink.annotation.Internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used on classes containing config optionss that enables the separation of options into different
 * tables based on key prefixes. A config option is assigned to a {@link ConfigGroup} if the option key matches
 * the group prefix. If a key matches multiple prefixes the longest matching prefix takes priority. An option is never
 * assigned to multiple groups. Options that don't match any group are implicitly added to a default group.
 *
 * 在含有配置optionss，使选项成基于关键的前缀不同的表分离类使用注释。 配置选项被分配到一个ConfigGroup如果选项键组前缀匹配。
 * 如果一个关键多个前缀匹配的最长前缀匹配优先。 一个选项是永远不会分配给多个组。 不符合任何组的选项默认添加到默认组。
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Internal
public @interface ConfigGroups {
	ConfigGroup[] groups() default {};
}
