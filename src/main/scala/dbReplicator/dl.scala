package dbReplicator

import com.amazonaws.services.s3.AmazonS3Client

object dl {
  def checkConnection(bucketName: String): Unit = {
    val s3 = new AmazonS3Client //Builder.defaultClient()
    val result:Boolean = s3.doesBucketExist(bucketName)
    assert(result == true, "ERROR: The bucket does not exist")
  }
}
