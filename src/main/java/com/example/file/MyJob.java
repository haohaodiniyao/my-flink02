package com.example.file;

import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MyJob {
    private String jobName;
    private Map<String,String> configMap;

    public MyJob(String jobName,String[] args) throws Exception{
        this.jobName = jobName;
        this.configMap = initConfigMap(args);
    }

    public JobExecutionResult execute() throws Exception{
        final StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        setCheckPoint(streamExecutionEnvironment,configMap);
        DataStreamSource<String> stringDataStreamSource = streamExecutionEnvironment.addSource(KafkaStreamSource.newStringFlinkKafkaConsumer(configMap));
        String output = configMap.get(Constants.OUTPUT_PATH);
        stringDataStreamSource.map(new ParseEvent()).uid("101").name("map:parse-event")
                .filter(Objects::nonNull).uid("102").name("filter:null")
                .addSink(MySink.newStreamingFileSink(output)).uid("103").name("sink:file")
                .setParallelism(1);
        return streamExecutionEnvironment.execute(jobName);
    }

    private Map<String,String> initConfigMap(String... args) throws Exception{
        final ParameterTool fromArgs = ParameterTool.fromArgs(args);
        final String env= fromArgs.getRequired(Constants.ENV);
        InputStream inputStream = MyJob.class.getClassLoader().getResourceAsStream("application-"+env+".properties");
        ParameterTool fromPropertiesFile = ParameterTool.fromPropertiesFile(inputStream);
        HashMap<String, String> configMap = Maps.newHashMap(fromPropertiesFile.toMap());
        configMap.putAll(fromArgs.toMap());
        return configMap;
    }

    private static void setCheckPoint(StreamExecutionEnvironment streamExecutionEnvironment,Map<String,String> configMap){
        long checkpointInterval = MapUtils.getLong(configMap, Constants.CHECKPOINT_INTERVAL,
                Constants.DEFAULT_LONG_VALUE);
        long minPauseBetweenCheckpoints = MapUtils.getLong(configMap, Constants.MIN_PAUSE_BETWEEN_CHECKPOINTS,
                Constants.DEFAULT_LONG_VALUE);
        long checkpointTimeout = MapUtils.getLongValue(configMap, Constants.CHECKPOINT_TIMEOUT,
                Constants.DEFAULT_LONG_VALUE);
        int maxConcurrentCheckpoints = MapUtils.getIntValue(configMap, Constants.MAX_CONCURRENT_CHECKPOINTS,
                Constants.DEFAULT_INT_VALUE);
        streamExecutionEnvironment.enableCheckpointing(checkpointInterval);
        CheckpointConfig checkpointConfig = streamExecutionEnvironment.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
        //超时时间
        checkpointConfig.setCheckpointTimeout(checkpointTimeout);
        //最大并发数量
        checkpointConfig.setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);
        //取消job触发checkpoint
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }
}
