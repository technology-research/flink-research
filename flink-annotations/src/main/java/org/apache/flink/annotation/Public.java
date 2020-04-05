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
 * 注释用于标记类作为公众，稳定的接口。 用于将类标记为公共，稳定接口的注释。
 * 类，方法和字段与此注释是跨次要版本（1.0，1.1，1.2）是稳定的。 换句话说，使用@public注解类的应用程序将编译针对同一主要版本的更新版本。
 * 只有主要版本（1.0，2.0，3.0）可以打破接口与此注释。
 *
 * <p>Classes, methods and fields with this annotation are stable across minor releases (1.0, 1.1, 1.2). In other words,
 * applications using @Public annotated classes will compile against newer versions of the same major release.
 *
 * <p>Only major releases (1.0, 2.0, 3.0) can break interfaces with this annotation.
 */
@Documented
@Target(ElementType.TYPE)
@Public
public @interface Public {}
