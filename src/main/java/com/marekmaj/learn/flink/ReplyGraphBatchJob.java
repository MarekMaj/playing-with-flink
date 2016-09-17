package com.marekmaj.learn.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ReplyGraphBatchJob {


    public static void main(String[] args) throws Exception {
        String path = ParameterTool.fromArgs(args).getRequired("data");

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // read messageId, sender email, replied-to-msg-id
        DataSet<Tuple3<String, String, String>> inputData =
                env.readCsvFile(path)
                        .lineDelimiter("##//##")
                        .fieldDelimiter("#|#")
                        .includeFields("101001")
                        .types(String.class, String.class, String.class);

        DataSet<Tuple3<String, String, String>> cleansed = inputData
                .map(data -> Tuple3.of(data.f0,
                        StringUtils.substringBeforeLast(
                                StringUtils.substringAfterLast(data.f1, "<"), ">"),
                        data.f2))
                .filter(data -> !"git@git.apache.org".equals(data.f1) && !"jira@apache.org".equals(data.f1));

        cleansed
                .join(cleansed).where(i1 -> i1.f2).equalTo(i2 -> i2.f0)    //reply-to == msg-id
                .with((i1, i2) -> Tuple2.of(i1.f1, i2.f1))                 //response-sender, original-mail sender
                .groupBy(0, 1)
                .reduceGroup(new CountRepliessReduceFunction())
                .print();
    }

    private static class CountRepliessReduceFunction implements GroupReduceFunction<Tuple2<String, String>, Tuple3<String, String, Long>> {

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
