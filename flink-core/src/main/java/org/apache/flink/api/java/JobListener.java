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

package org.apache.flink.api.java;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;

/**
 * Hooks for job submission and execution which is invoked in client side.
 * 在客户端调用作业提交和执行的钩子函数
 */
public interface JobListener {

	/**
	 * 在作业提交之后
	 * @param jobId
	 */
	void onJobSubmitted(JobID jobId);

	/**
	 * 在作业执行之后
	 * @param jobResult
	 */
	void onJobExecuted(JobExecutionResult jobResult);

	/**
	 * 在作业取消之后
	 * @param jobId
	 * @param savepointPath
	 */
	void onJobCanceled(JobID jobId, String savepointPath);
}
