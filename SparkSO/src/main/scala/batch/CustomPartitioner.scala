package batch

import org.apache.spark.HashPartitioner

class CustomPartitioner(partitions: Int) extends HashPartitioner(partitions) {
    override def getPartition(key: Any): Int = key match {
        case k: (String, String) => super.getPartition(k._2)
        case _ => super.getPartition(key)
    }
}