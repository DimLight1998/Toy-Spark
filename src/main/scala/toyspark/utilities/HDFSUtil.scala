package toyspark.utilities

import java.io._
import java.util.concurrent.CyclicBarrier

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.zookeeper.common.IOUtils

import scala.collection.mutable.ArrayBuffer

/**
  * HDFS operator
  */
object HDFSUtil {
  var hdfsUrl      = ""
  var realUrl      = ""
  var coreSitePath = ""

  /**f
    * make a new dir in the hdfs
    *
    * @param dir the dir may like '/tmp/testdir'
    * @return boolean true-success, false-failed
    */
  def mkdir(dir: String): Boolean = {
    var result = false
    if (StringUtils.isNotBlank(dir)) {
      realUrl = hdfsUrl + dir
      val config = new Configuration()
      config.addResource(new Path(coreSitePath))
      val fs = FileSystem.get(config)
      if (!fs.exists(new Path(realUrl))) {
        fs.mkdirs(new Path(realUrl))
      }
      fs.close()
      result = true
    }
    result
  }

  /**
    * delete a dir in the hdfs.
    * if dir not exists, it will throw FileNotFoundException
    *
    * @param dir the dir may like '/tmp/testdir'
    * @return boolean true-success, false-failed
    *
    */
  def deleteDir(dir: String): Boolean = {
    var result = false
    if (StringUtils.isNotBlank(dir)) {
      realUrl = hdfsUrl + dir
      val config = new Configuration()
      config.addResource(new Path(coreSitePath))
      val fs = FileSystem.get(config)
      fs.delete(new Path(realUrl), true)
      fs.close()
      result = true
    }
    result
  }

  /**
    * list files/directories/links names under a directory, not include embed
    * objects
    *
    * @param dir a folder path may like '/tmp/testdir'
    * @return List[String] list of file names
    */
  def listAll(dir: String): List[String] = {
    val names = ArrayBuffer[String]()
    if (StringUtils.isNotBlank(dir)) {
      realUrl = hdfsUrl + dir
      val config = new Configuration()
      config.addResource(new Path(coreSitePath))
      val fs    = FileSystem.get(config)
      val stats = fs.listStatus(new Path(realUrl))
      for (i <- stats.indices) {
        if (stats(i).isFile) {
          names += stats(i).getPath.toString
        } else if (stats(i).isDirectory) {
          names += stats(i).getPath.toString
        } else if (stats(i).isSymlink) {
          names += stats(i).getPath.toString
        }
      }
    }
    names.toList
  }

  /**
    * upload the local file to the hds,
    * notice that the path is full like /tmp/test.txt
    * if local file not exists, it will throw a FileNotFoundException
    *
    * @param localFile local file path, may like F:/test.txt or /usr/local/test.txt
    *
    * @param hdfsFile hdfs file path, may like /tmp/dir
    * @return boolean true-success, false-failed
    *
    **/
  def uploadLocalFile2HDFS(localFile: String, hdfsFile: String): Boolean = {
    var result = false
    if (StringUtils.isNotBlank(localFile) && StringUtils.isNotBlank(hdfsFile)) {
      realUrl = hdfsUrl + hdfsFile
      val config = new Configuration()
      config.addResource(new Path(coreSitePath))
      val hdfs = FileSystem.get(config)
      val src  = new Path(localFile)
      val dst  = new Path(realUrl)
      hdfs.copyFromLocalFile(src, dst)
      hdfs.close()
      result = true
    }
    result
  }

  /**
    * create a new file in the hdfs. notice that the toCreateFilePath is the full path
    *  and write the content to the hdfs file.

    * create a new file in the hdfs.
    * if dir not exists, it will create one
    *
    * @param newFile new file path, a full path name, may like '/tmp/test.txt'
    * @param content file content
    * @return boolean true-success, false-failed
    **/
  def createNewHDFSFile(newFile: String, content: Array[Byte], barrier: Option[CyclicBarrier] = None): Boolean = {
    var result = false
    if (StringUtils.isNotBlank(newFile) && null != content) {
      realUrl = hdfsUrl + newFile
      val config = new Configuration()
      config.addResource(new Path(coreSitePath))
      val hdfs = FileSystem.get(config)
      val os   = hdfs.create(new Path(realUrl))
      os.write(content)
      barrier match {
        case Some(br) => br.await()
        case None     =>
      }
      os.close()
      hdfs.close()
      result = true
    }
    result
  }

  /**
    * delete the hdfs file
    *
    * @param hdfsFile a full path name, may like '/tmp/test.txt'
    * @return boolean true-success, false-failed
    */
  def deleteHDFSFile(hdfsFile: String): Boolean = {
    var result = false
    if (StringUtils.isNotBlank(hdfsFile)) {
      realUrl = hdfsUrl + hdfsFile
      val config = new Configuration()
      config.addResource(new Path(coreSitePath))
      val hdfs      = FileSystem.get(config)
      val path      = new Path(realUrl)
      val isDeleted = hdfs.delete(path, true)
      hdfs.close()
      result = isDeleted
    }
    result
  }

  /**
    * read the hdfs file content
    *
    * @param hdfsFile a full path name, may like '/tmp/test.txt'
    * @return byte[] file content
    */
  def readHDFSFile(hdfsFile: String, barrier: Option[CyclicBarrier] = None): Array[Byte] = {
    var result = new Array[Byte](0)
    if (StringUtils.isNotBlank(hdfsFile)) {
      realUrl = hdfsUrl + hdfsFile
      val config = new Configuration()
      config.addResource(new Path(coreSitePath))
      val hdfs = FileSystem.get(config)
      val path = new Path(realUrl)
      if (hdfs.exists(path)) {
        val inputStream = hdfs.open(path)
        val stat        = hdfs.getFileStatus(path)
        val length      = stat.getLen.toInt
        val buffer      = new Array[Byte](length)
        inputStream.readFully(buffer)
        barrier match {
          case Some(br) => br.await()
          case None     =>
        }
        inputStream.close()
        hdfs.close()
        result = buffer
      }
    }
    result
  }

  /**
    * append something to file dst
    *
    * @param hdfsFile a full path name, may like '/tmp/test.txt'
    * @param content string
    * @return boolean true-success, false-failed
    */
  def append(hdfsFile: String, content: String): Boolean = {
    var result = false
    if (StringUtils.isNotBlank(hdfsFile) && null != content) {
      realUrl = hdfsUrl + hdfsFile
      val config = new Configuration()
      config.addResource(new Path(coreSitePath))
      config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
      config.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
      val hdfs = FileSystem.get(config)
      val path = new Path(realUrl)
      if (hdfs.exists(path)) {
        val inputStream  = new ByteArrayInputStream(content.getBytes())
        val outputStream = hdfs.append(path)
        IOUtils.copyBytes(inputStream, outputStream, 4096, true)
        outputStream.close()
        inputStream.close()
        hdfs.close()
        result = true
      }
    } else {
      HDFSUtil.createNewHDFSFile(hdfsFile, SerializationUtils.serialize(content))
      result = true
    }
    result
  }

}
