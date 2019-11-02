### stream 


datastream 的定义不同于dataset, dataset 作为batch， 类似于spark的batch定义， 对数据源的操作，如map，fliter，groupBy flatmap等

都是对于同一个文件分片处理，如果没必要分片的话。所以dataset的定义算子比较丰富。

![dataset](Dataset.png)

而datastream 的子类则比较单一了

![datastream](DataStream.png)

dataStream的算子都 基本都是由 keyedStram 和 SingleOutputStreamOperator定义

keyed 用于自定义分区, 类似于kafka中的key, hash（分片算法之一）分发到不同的下游数据。
SingleOutputStreamOperator 就是对本数据流中的数据处理 如  map, flatmap, filter。
SplitStream 由input 自定义需要的数据，自定义tag，向下游选择性的分发数据。
