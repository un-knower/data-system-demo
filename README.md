Build With Dependencies
	mvn assembly:assembly
Run
	java -cp xxx-jar-with-denpendencies.jar class


2016-9-2
简要描述一下项目(靠回忆，有点忘了)：

	多生产者--->队列--->单消费者--->log(写入磁盘)-||-监控log-->监控者(timeout超时/bufferSize溢出 就批量写到redis)---->磁盘
	用到redis了，还用到了redis的并发，用在哪里记不住了

【第一部分】

	多线程生产，单线程消费，生产者消费者间通过队列，支持size&timeout触发，能够正常启停，消息不重不漏

1.关键问题：
	1)消费队列设计(两套方案就是队列设计思想不同)
	2)系统停止流程(见2-A-shutdown流程)
	3)多生产者访问文件冲突(-lock,-done，见代码)
2.两套方案：
A.两级缓存消费队列，实现生产消费的并行执行，批量处理，速度快。方案介绍：
类图:
	Producer.class--[has a]------>QueueConsumer.class----[has a]-->SinkThread.class
	|directory         |buffer                               |
	|thread_num        |toSink/toSinkSize/toSinklock         |
	|thread_alive      |returnBfferList                      |
	|shutdown          |sinkThread                           |
	|qcon              |batchFactor,timeout,buffersize,output|

数据流图:
	     源有数据           batchFactor         bufferSize/timeout       触发流动条件
	Data---------->buffer-----lock------>toSink------------------>File  数据流动(流经)

	   addElement()     flushBufferList()         sinkToFile()          方法(数据流动调用)          
	                                          |---SinkThread----------| 类
	                                             InterruptedException   关(各类作用于数
	|---Producer--|----QueueConsumer----------------------------------| 系 据流哪部分)

生产者特点：
1)Producer的Thread alive控制
2)Producer的并发生产控制"-lock""-done"
消费者特点：
1)消费队列：二级缓存(详细间代码或Blog<[生产者有独立桶]一种生产者消费者实现逻辑>)
2)一线程一stamp
3)bufferSize&timeout&batchFactor触发条件
4)回收List

shutdown流程：多线程合作，三身份互动(生产者，消费队列，sink线程)
1.调用方法Producer.stop(),触发信号stopSignal
2.生产者收到信号，完成当前文件后flushBufferList()，然后结束
3.所有生产者线程结束
4.调用consumer.flushSinkFile(),将buffer内容flush到磁盘
5.调用consumer.stop()，触发stop signal
6.SinkThread收到信号stop

B.使用线程安全队列ConcurrentLinkedQueue
1.特点：
	a.自定义类ConsumerNoticeConcurrentLinkedQueue继承自ConcurrentLinkedQueue
		1)override部分方法，使添加元素时统计size，若>bufferSize，则通知消费，以实现buffersize触发
		2)size是无锁自增减的，使用CAS
	b.分离了Consumer和Queue
	c.Producer和A一样
	d.仍然支持buffersize&timeout
2.类关系：
         --has a-->                                     --has a--> 
Producer------------ConsumerNoticeConcurrentLinkedQueue------------Consumer
                                                        <-has a--
互相has a确实奇葩，是为了能实现buffersize触发消费。

【第二部分】
	
	监控日志目录，size&timeout触发收集日志并转存，能够正常停止。

描述图：
	CollectorThread     SinkThread         MergeLogThread
	recover();             sink            1.停止所有sinkThread
	while(){                               2.merge
		scan                               3.完成
		......
	}

特点：
1.支持崩溃恢复。
	1)引入日志，每次sink都有record，一定时间或量后还会增加一个check point。
	record只记录该文件的offset转存到了哪，check point记录当前时间点所有文件的所有offset。
	2)每次系统启动都从日志回复offsets，如果日志不是以check point结尾说明上次没有正常关闭，就从上报服务处查出当前的offset进度，用于恢复当前offsets，并增加一个此刻的check point。
2.scan实现size&timeout方法比较粗暴，以timeout/CollectorThread     SinkThread         MergeLogThread
	recover();             sink            1.停止所有sinkThread
	while(){                               2.merge
		scan                               3.完成
		......
	}n的时间醒来，如果满一个timeout(超时)或size>规定(溢出)则启动sink线程。
3.MergeLog比较鸡肋，因为有check point就足够了，之前record可以删掉，没必要留着。