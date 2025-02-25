# Hadoop 集群


HDFS適合場景:
-  一次寫入 多次讀出
<br />

HDFS不適合場景:
-  低延遲數據訪問 匹如毫秒級數據存儲
-  大量小文件
-  不支持併發寫入
-  僅支持數據追加 不支持隨機修改
<br />

## 常用端口:
#### Hadoop3X:
-  HDFS NameNode 內部通訊端口：8020/9000/9820<br />
-  HDFS NameNode 對用戶的查詢端口：9870<br />
-  Yarn查看任務運行情況的端口：8088<br />
-  歷史服務器：19888<br />


#### Hadoop2X:
-  HDFS NameNode 內部通訊端口：8020/9000<br />
-  HDFS NameNode 對用戶的查詢端口：50070<br />
-  Yarn查看任務運行情況的端口：8088<br />
-  歷史服務器：19888<br />

## 常用的配置文件:
#### Hadoop3X:
core-site.xml<br />
hdfs-site.xml<br />
yarn.site.xml<br />
mapred-site.xml<br />
workers<br />
#### Hadoop2X:
core-site.xml<br />
hdfs-site.xml<br />
yarn.site.xml<br />
mapred-site.xml<br />
slaves<br />

## 重點:
HDFS 文件塊大小:
如果塊設置過大:
- 單節點讀取 無法並行，容易形成瓶頸

如果塊設置過小:
- 尋址時間增大
- Client 端需要組裝太多小塊，增加網路開銷

建議: 中小企業(128m) 大企業(256m)


## HDFS讀寫流程：TODO


## 指令:
啟動歷史紀錄:
mapred --daemon start historyserver
<br />
創建路徑:
hadoop fs -mkdir /wcinput<br />
上傳檔案:
hadoop fs -put /test/17806d92be573c003896fec879b06910.png /wcinput<br />

測試 對 HDFS中的 /wcinput 目錄進行詞頻統計，並將結果輸出到 /wcoutput 目錄 :
hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar wordcount /wcinput /wcoutput
<br />

## UI 介面:[config.env](config.env)
查看任務詳情
http://localhost:8088/cluster/apps<br />
訪問HDFS集群的Namenode頁面
http://localhost:9870/<br />


## 注意事項:
上傳下載使用 UI URL ip會動態帶入 外部訪問使用 docker 需要使用暴露與端口
<br />
Datanode 無暴露端口需要使用指令查詢docker容器ip
<br />
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' e66558c3d824

# 配置擋使用時備註需刪除:
# config.env:
```
# core-site.xml 配置
CORE-SITE.XML_fs.default.name=hdfs://namenode  # HDFS 默認命名空間地址
CORE-SITE.XML_fs.defaultFS=hdfs://namenode  # HDFS 默認文件系統
CORE-SITE.XML_hadoop.http.staticuser.user=root  # 設置 HTTP 用戶為 root
CORE-SITE.XML_hadoop.tmp.dir=/tmp/hadoop-root  # Hadoop 臨時目錄

# hdfs-site.xml 配置
HDFS-SITE.XML_dfs.namenode.rpc-address=namenode:8020  # NameNode RPC 服務器地址
HDFS-SITE.XML_dfs.replication=2  # HDFS 副本因子（設置為 2 表示每個數據塊有兩個副本）

# mapred-site.xml 配置
MAPRED-SITE.XML_mapreduce.framework.name=yarn  # MapReduce 計算框架使用 YARN
MAPRED-SITE.XML_yarn.app.mapreduce.am.env=HADOOP_MAPRED_HOME=${HADOOP_HOME}  # 設置 AM（Application Master）環境變量
MAPRED-SITE.XML_mapreduce.map.env=HADOOP_MAPRED_HOME=${HADOOP_HOME}  # 設置 Map 任務環境變量
MAPRED-SITE.XML_mapreduce.reduce.env=HADOOP_MAPRED_HOME=${HADOOP_HOME}  # 設置 Reduce 任務環境變量
MAPRED-SITE.XML_mapreduce.jobhistory.address=0.0.0.0:10020  # JobHistory Server 地址
MAPRED-SITE.XML_mapreduce.jobhistory.webapp.address=0.0.0.0:19888  # JobHistory Server Web UI 端口

# yarn-site.xml 配置
YARN-SITE.XML_yarn.resourcemanager.hostname=resourcemanager  # YARN ResourceManager 主機名
YARN-SITE.XML_yarn.nodemanager.pmem-check-enabled=true  # 啟用物理內存檢查
YARN-SITE.XML_yarn.nodemanager.delete.debug-delay-sec=600  # 刪除容器日誌的延遲時間（單位：秒）
YARN-SITE.XML_yarn.nodemanager.vmem-check-enabled=true  # 啟用虛擬內存檢查
YARN-SITE.XML_yarn.nodemanager.aux-services=mapreduce_shuffle  # 設置輔助服務為 mapreduce_shuffle
YARN-SITE.XML_yarn.nodemanager.resource.cpu-vcores=4  # 設置每個 NodeManager 提供的 CPU 核數
YARN-SITE.XML_yarn.log-aggregation-enable=true  # 啟用 YARN 日誌聚合
YARN-SITE.XML_yarn.log.server.url=http://namenode:19888/jobhistory/logs  # 指定 YARN 日誌伺服器 URL
YARN-SITE.XML_yarn.log-aggregation.retain-seconds=604800  # 指定日誌在 HDFS 中保留的時間（秒）
YARN-SITE.XML_yarn.application.classpath=opt/hadoop/etc/hadoop:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/yarn:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/*
# 以上設置 YARN 任務運行時的 CLASSPATH，確保 Hadoop 組件可用

# capacity-scheduler.xml 配置
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.maximum-applications=10000  # 最大支持的應用數量
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.maximum-am-resource-percent=0.1  # AM 資源最大佔比
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.resource-calculator=org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator  # 資源計算方式
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.queues=default  # 根隊列設置
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.capacity=100  # 默認隊列的容量（百分比）
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.user-limit-factor=1  # 用戶最大可用資源係數
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.maximum-capacity=100  # 隊列最大容量
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.state=RUNNING  # 隊列當前狀態
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.acl_submit_applications=*  # 允許所有用戶提交應用
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.root.default.acl_administer_queue=*  # 允許所有用戶管理隊列
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.node-locality-delay=40  # 本地性延遲（任務優先在本地運行）
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.queue-mappings=  # 隊列映射（留空表示無映射）
CAPACITY-SCHEDULER.XML_yarn.scheduler.capacity.queue-mappings-override.enable=false  # 是否啟用隊列映射重寫

```



## 集群壓測 (影響HDFS 性能: 網速 硬碟讀寫速度 )
hadoop自帶壓測tests.jar:
/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3-tests.jar




### 寫壓測:
提交10個文件，開啟10個MapTask，每個MapTask開始向當前節點HDFS寫數據，每個Map會記錄下寫的時間和平均速度，而ReduceTask會匯總每個MapTask的寫入時間和平均速度。
<br />

hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3-tests.jar TestDFSIO -write -nrFiles 10 -fileSize 128MB
<br />
-write表示啟動寫測試，
<br />
-nfFiles 10表示提交10個文件，對應生成MapTask的數量，而提交的文件數，一般是集群CPU總核數 - 1。
<br />
-fileSize 128MB表示每個文件大小是128MB。
<br />

2021-02-09 10:43:16,853 INFO fs.TestDFSIO: ----- TestDFSIO ----- : write <br />
2021-02-09 10:43:16,854 INFO fs.TestDFSIO:             Date & time: Tue Feb 09 10:43:16 CST 2021 <br />
2021-02-09 10:43:16,854 INFO fs.TestDFSIO:         Number of files: 10 <br />
2021-02-09 10:43:16,854 INFO fs.TestDFSIO:  Total MBytes processed: 1280 <br /> 
2021-02-09 10:43:16,854 INFO fs.TestDFSIO:       Throughput mb/sec: 1.61 <br />
2021-02-09 10:43:16,854 INFO fs.TestDFSIO:  Average IO rate mb/sec: 1.9 <br />
2021-02-09 10:43:16,854 INFO fs.TestDFSIO:   IO rate std deviation: 0.76 <br />
2021-02-09 10:43:16,854 INFO fs.TestDFSIO:      Test exec time sec: 133.05 <br />
2021-02-09 10:43:16,854 INFO fs.TestDFSIO:


- Throughput : 所有數據量累加 / 所有數據寫時間累加，即集群整體吞吐量
- Average IO rate :所有平均速度累加 / 10，即平均MapTask的吞吐量
- IO rate std deviation : 方差，反應各個MapTask處理的差值，越小越均衡

測試結果分析:
<br />
以上面的輸出為例，我們的壓測后速度是1.61，每個文件默認3個副本，但由於副本1，即文件本身都在節點1上，所以我們在寫數據的時候，每個文件相當於只寫了2個副本，即節點2和節點3上。
<br />
所以參與測試的文件就是20個。（如果客戶端不在集群節點上，那麼就三個副本都參與計算。就是30個文件了）
<br />
實測速度：1.61X20=32M/s
<br />
三台服務器的總帶寬：12.5X3=37M/s
<br />
基本相當於所有網絡資源都已經用滿。如果實測速度遠遠小於網絡速度，且不能滿足工作需求，那麼可以採用固態硬盤或者增加磁盤個數等。



### 讀壓測:

hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3-tests.jar TestDFSIO -read -nrFiles 10 -fileSize 128MB

2021-02-09 11:34:15,847 INFO fs.TestDFSIO: ----- TestDFSIO ----- : read <br />
2021-02-09 11:34:15,847 INFO fs.TestDFSIO:             Date & time: Tue Feb 09 11:34:15 CST 2021 <br />
2021-02-09 11:34:15,847 INFO fs.TestDFSIO:         Number of files: 10 <br />
2021-02-09 11:34:15,847 INFO fs.TestDFSIO:  Total MBytes processed: 1280 <br />
2021-02-09 11:34:15,848 INFO fs.TestDFSIO:       Throughput mb/sec: 200.28 <br />
2021-02-09 11:34:15,848 INFO fs.TestDFSIO:  Average IO rate mb/sec: 266.74 <br />
2021-02-09 11:34:15,848 INFO fs.TestDFSIO:   IO rate std deviation: 143.12 <br />
2021-02-09 11:34:15,848 INFO fs.TestDFSIO:      Test exec time sec: 20.83 <br />

讀的速度是很快的，且讀取文件速度大於網絡帶寬。這是由於目前只有三台服務器，且有三個副本，數據讀取就近原則，相當於都是讀取的本地磁盤數據，沒有走網絡。

刪除測試生成的數據:
hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3-tests.jar TestDFSIO -clean


