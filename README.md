# Spark_HLL

SparkSQL supports fast hyperloglog based counting, but there is no way to save the registers and do incremental rollups, which is a quite common use case.
Here I used SparkSQL's powerful and flexible API, plus the HyperLogLogPlusPlus implentation to achieve that.
