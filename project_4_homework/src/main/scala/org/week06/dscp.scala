package org.week06

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object dscp {
  type OptionMap = Map[Symbol, Any]

  def main(args: Array[String]): Unit = {
    if(args.length < 2) {
      println("参数个数不对，至少包含源和目标目录")
      sys.exit(1)
    }
    // 获取所有参数
    val argsList: List[String] = args.toList
    val options = parseOptions(Map(), argsList)
    println(options)
    val sourceFolder = String.valueOf(options(Symbol("infile")))
    val targetFolder = String.valueOf(options(Symbol("outfile")))
    val concurrency = (options(Symbol("maxconcurrency"))).toString.toInt
    val ignoreFailure = (options(Symbol("ignoreFailure"))).toString.toInt


    val sparkConf: SparkConf = new SparkConf().setAppName("dscp").setMaster("local[*]")
    val sparkContext: SparkContext = SparkContext.getOrCreate(sparkConf)
    val sb = new StringBuilder()
    val fileNames = new ListBuffer[String]()

    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://master:8020")

    getFileOrPath(conf, sourceFolder, fileNames)
    fileNames.foreach(fileName => {
      try{
        sparkContext.textFile(fileName, concurrency).saveAsTextFile(fileName.replace(sourceFolder, targetFolder))
      } catch {
        case t: Throwable => t.printStackTrace()
          if(ignoreFailure == 0) {
            throw new Exception("复制文件 " + fileName +" 失败")
          }
      }
    })
  }

  def getFileOrPath(hconf: Configuration, path:String, filePaths:ListBuffer[String]): Unit = {
    val fileList: Array[FileStatus] = FileSystem.get(hconf).listStatus(new Path(path))
    fileList.foreach(
      fStatus => {
        if(!fStatus.isDirectory) {
          filePaths += fStatus.getPath.toString
        } else if(fStatus.isDirectory) {
          getFileOrPath(hconf, fStatus.getPath.toString, filePaths)
        }
      }
    )
  }

  def parseOptions(map:OptionMap, list: List[String]):OptionMap={
    def isSwitch(s : String) =( s(0) == '-')

    list match {
      case Nil => map
      case "-i" ::value=> parseOptions(map ++ Map(Symbol("ignoreFailure") -> 1), list.tail)
      case "-m" ::value::tail => parseOptions(map ++ Map(Symbol("maxconcurrency") -> value.toInt), tail)
      case string::Nil => parseOptions(map ++ Map(Symbol("outfile") -> string), list.tail)
      case string :: tail => parseOptions(map ++ Map(Symbol("infile") -> string), tail)
      case option::opt2::tail if isSwitch(opt2) => {
        println("未知的参数：" + option)
        sys.exit(1)
      }
    }
  }
}
