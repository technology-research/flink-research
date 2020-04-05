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
 *
 */

package org.apache.flink.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Annotation to mark classes and methods for public use, but with evolving interfaces.
 *
 * <p>Classes and methods with this annotation are intended for public use and have stable behavior.
 * However, their interfaces and signatures are not considered to be stable and might be changed
 * across versions.
 *
 * <p>This annotation also excludes methods and classes with evolving interfaces / signatures
 * within classes annotated with {@link Public}.
 *
 * 标注来标记的类和方法，供市民使用，但与不断变化的接口。
 * 类和方法与此注释旨在供公众使用，并有稳定的表现。 然而，它们的接口和签名不被认为是稳定的，可能不同版本进行更改。
 * 这个注解也排除方法和类与注解的类内演变接口/签名Public 。
 */
@Documented
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR })
@Public
public @interface PublicEvolving {
}
