package com.marekmaj.learn.flink;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;


public class TableApiCountJob {

    public static void main(String[] args) throws Exception {
        String path = ParameterTool.fromArgs(args).getRequired("data");
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        // read timestamp and sender fields only
        DataSet<Tuple2<String, String>> inputData =
                env.readCsvFile(path)
                        .lineDelimiter("##//##")
                        .fieldDelimiter("#|#")
                        .includeFields("01100")
                        .types(String.class, String.class);

        DataSet<Tuple2<String, String>> cleansed = inputData
                .map(input -> Tuple2.of(EmailsDataUtils.MonthFromStringTimestamp.from(input.f0), input.f1))
                .map(input -> Tuple2.of(input.f0, EmailsDataUtils.clearEmailAddress(input.f1)))
                .filter(input -> EmailsDataUtils.isSenderNotABot(input.f1));


        Table table = tEnv.fromDataSet(cleansed, "month, sender");

        Table countedMailsGroupedByMonthAndUser = table.groupBy("month, sender").select("month, sender, month.count as cnt");

        Table maxMailsGroupedByMonth = countedMailsGroupedByMonthAndUser
                .groupBy("month")
                .select("month as month0, cnt.max as max_count")
                .join(countedMailsGroupedByMonthAndUser).where("month = month0 && cnt = max_count")
                .select("month, sender, max_count")
                .orderBy("month");


        TypeInformation<Tuple3<String, String, Long>> type = TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>(){});
        DataSet<Tuple3<String, String, Long>> result = tEnv.toDataSet(maxMailsGroupedByMonth, type);

        result.print();
    }
}
