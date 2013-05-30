/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.mapreduce.util;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.aspectj.lang.Aspects;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

import org.kiji.schema.util.LogTimerAspect;
import org.kiji.schema.util.LoggingInfo;

/**
 * This aspect is invoked after the cleanup function in a mapreduce job. It
 * accesses logging information gathered by the LogTimerAspect in kiji schema and
 * serializes it to a local file.
 */
@Aspect
public class SerializeLoggerAspect {
  private LogTimerAspect mLogTimerAspect;

  /**
   * Default constructor. Initializes the pid of the JVM running the tool
   * and the singleton LogTimerAspect for this JVM instance.
   */
  protected SerializeLoggerAspect() {
    if (Aspects.hasAspect(LogTimerAspect.class)) {
      mLogTimerAspect = Aspects.aspectOf(LogTimerAspect.class);
    } else {
      throw new RuntimeException("Log Timer aspect not found!");
    }
  }

  /**
   * Pointcut attached to cleanup code in Map/Reduce tasks.
   */
  @Pointcut("execution(* org.kiji.mapreduce.KijiMapper.cleanup(..)) ||"
      + "execution(* org.kiji.mapreduce.KijiReducer.cleanup(..))")
  protected void mrCleanupPoint(){
  }

  /**
   * Advice for running after any functions that match PointCut "writeResultsLocal".
   *
   * @param thisJoinPoint The joinpoint that matched the pointcut.
   * @throws IOException If the writes to hdfs fail.
   */
  @AfterReturning("mrCleanupPoint() && !cflowbelow(mrCleanupPoint())")
  public void afterMRCleanup(final JoinPoint thisJoinPoint) throws IOException {
    TaskInputOutputContext context = (TaskInputOutputContext)thisJoinPoint.getArgs()[0];

    Path parentPath = new Path(context.getWorkingDirectory(), "kijistats");
    FileSystem fs = FileSystem.get(context.getConfiguration());
    fs.mkdirs(parentPath);
    Path path = new Path(parentPath, context.getTaskAttemptID().toString());
    OutputStreamWriter out = new OutputStreamWriter(fs.create(path, true), "UTF-8");
    try {
      out.write("Job Name, Job ID, Task Attempt, Function Signature, Aggregate Time (nanoseconds), "
          + "Number of Invocations, Time per call (nanoseconds)\n");
      HashMap<String, LoggingInfo> signatureTimeMap =
          mLogTimerAspect.getSignatureTimeMap();
      for (Map.Entry<String, LoggingInfo> entrySet: signatureTimeMap.entrySet()) {
        out.write(context.getJobName() + ", "
            + context.getJobID() + ", "
            + context.getTaskAttemptID() + ", "
            + entrySet.getKey() + ", "
            + entrySet.getValue().toString() + ", "
            + entrySet.getValue().perCallTime().toString() + "\n");
      }
    } finally {
      out.close();
    }
  }
}
