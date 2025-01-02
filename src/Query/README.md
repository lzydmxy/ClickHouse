本目录放京东内部的优化器代码，和社区目录结构区分开来
1、Common，公共的通用类
2、Analyzer，分析器、改写、方言相关代码
3、QueryPlan，优化器用到新增的Plan和Step结构相关代码
4、Optimizer，优化器，RBO、CBO等
5、Executor，执行器，主要是分布式调度和执行
6、Schedule，后台任务调度器，暂时可能用不到
7、Protos，交互的PB文件