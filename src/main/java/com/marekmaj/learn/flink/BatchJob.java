package com.marekmaj.learn.flink;

/**
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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Skeleton for a Flink Batch Job.
 *
 * For a full example of a Flink Batch Job, see the WordCountJob.java file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/learn-flink-1.0-SNAPSHOT.jar
 * From the CLI you can then run
 * 		./bin/flink run -c com.marekmaj.BatchJob target/learn-flink-1.0-SNAPSHOT.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class BatchJob {

    public static void main(String[] args) throws Exception {
        String path = ParameterTool.fromArgs(args).getRequired("data");
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // read timestamp and sender fields only
        DataSet<Tuple2<String, String>> inputData =
                env.readCsvFile(path)
                        .lineDelimiter("##//##")
                        .fieldDelimiter("#|#")
                        .includeFields("01100")
                        .types(String.class, String.class);

        inputData
                .map(input -> Tuple2.of(EmailsDataUtils.MonthFromStringTimestamp.from(input.f0), input.f1))
                .groupBy(0, 1)
                .reduceGroup(new CountEmailsReduceFunction()).print();
    }

    private static class CountEmailsReduceFunction implements GroupReduceFunction<Tuple2<String, String>, Tuple3<String, String, Long>> {

        @Override
        public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple3<String, String, Long>> out) throws Exception {
            Tuple3<String, String, Long> result = StreamSupport.stream(values.spliterator(), false)
                    // eh...
                    .collect(Collectors.reducing(
                            Tuple3.of("", "", 0L),
                            t -> Tuple3.of(t.f0, t.f1, 1L),
                            (u, u2) -> Tuple3.of(u2.f0, u2.f1, u.f2 + u2.f2)));

            out.collect(result);
        }
    }
}
