#### 概述
基于flink1.13做的测试，主要分为flinksql、flink dataStream以及后续基于sql和dataStream做的一些项目。
其中flinksql模块中相关的sql文件放在sql/flinksql_ddl目录下，相应的sql文件解析来自朱慧培大佬的flink-streaming-platform-web
项目中封装好的sql解析部分。

#### flinksql
```
1.flinksql几种常用连接器的使用测试
2.自定义udf函数
3.自定义sql序列化类
4.自定义connector
5.自定义catalog
6.flinksql窗口函数测试
7.flinksql join测试
```

#### flink dataStream
```
1.wordcount测试
2.window函数测试
3.自定义source测试
4.自定义sink测试
5.flink状态使用测试
6.自定义metrics测试
7.flinkcdc使用测试
```





