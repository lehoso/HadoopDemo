# 注意事项

## windows环境下需要使用uitls.exe

可从github下载，并且要设置环境变量 HADOOP_HOME=Location 【安装解压的位置】 PATH添加：%HADOOP_HOME%\bin 和 %HADOOP_HOME%\sbin

Hadoop集群是3.3，建议pom.xml的hadoop依赖包也是3.3

## MapReduce单词统计

mapred-default.xml mapred-site.xml 调优化调参

## MapReduce 倒排索引

InvertedIndex