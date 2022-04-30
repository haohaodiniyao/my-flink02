消费kafka
按日期分桶写入文件
参数可配置
--env prod
--bootstrap.servers
--auto.offset.reset latest
--flink.partition-discovery-interval-millis 5000
--enable.auto.commit true
--auto.commit.interval.ms 2000
--kafka.topic
--checkpoint.interval 300000
--min.pause.between.checkpoints 300000
--checkpoint.timeout 300000
--max.concurrent.checkpoints 1
--output_path 
