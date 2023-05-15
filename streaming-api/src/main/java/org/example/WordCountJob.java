package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WordCountJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("./streaming-api/src/main/resources/Bibel.txt")).build();

        DataStream<String> wordStream = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "bibel");

        DataStream<Tuple2<String, Integer>> wordCount = wordStream
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (words, collector) -> {
                    for (String word : words.split("\\W+")) {
                        if (word.length() > 0) {
                            collector.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(50)))
                .sum(1);

        final FileSink<Tuple2<String, Integer>> sink = FileSink
                .forRowFormat(new Path("test"), new SimpleStringEncoder<Tuple2<String, Integer>>("UTF-8"))
                .build();

        wordCount.sinkTo(sink);

        env.execute("Fraud Detection");
    }
}
