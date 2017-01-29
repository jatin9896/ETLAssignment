import java.io.{File, FileNotFoundException, IOException, PrintWriter}

import scala.io.Source
/**
  * Created by jatin on 28/1/17.
  */
trait ETLFileOperations extends DBOperations{
  def inputpath:String
  def outputpath:String

  private def toUpper(filename:String ):String={
    val writer = new PrintWriter(new File(outputpath+"/"+filename))
    println(s"File to be modified : ${inputpath+"/"+filename}")
    try {
      Source.fromFile(inputpath + "/" + filename).foreach {
        x =>
          if (x.isLetter) writer.write(x.toUpper)
          else
            writer.write(x)
      }
    }
    catch{
      case ex: FileNotFoundException=> println("File not found")
        case ex: IOException=>
        println("Input Output Exception")
      }
    finally {
      writer.close()
    }
    outputpath+"/"+filename
  }
  def captialiseAllFile(format:String): String =
  {
    val listoffiles=getAllFileWithFormat(format)
    if(listoffiles.size==0)
      println("No Files To Be Modified")
    val outputdata=listoffiles.map(toUpper(_))
    outputpath
  }
  def getAllFileWithFormat(format:String ) ={
     val filesHere = (new java.io.File(inputpath)).listFiles
        val outputfile= for (file <- filesHere if file.getName.endsWith(format)) yield(file.getName)
        outputfile.toList
     }
    def getData(filename:String):String = Source.fromFile(inputpath+"/"+filename).mkString
    def printData(filename:String)={
    Source.fromFile(filename).foreach {
      print
    }
  }
  def listToMap(filename:String):Map[String,Int] = {
    val filedatalist = fileWordList(filename)
    val temp = filedatalist.groupBy(x => x)
    val temp1 =(temp.map(x => x._1).zip(temp.map(_._2.size))).toMap[String,Int]
    temp1
  }

 private def fileWordList(filename:String):List[String]={
    val filedata:String =getData(filename)
    val datasplit=filedata.split(" ").toList
    val temp=datasplit.map(_.replaceAll("[^\\w]",""))
    temp.filter(_.length > 0)
  }
}


object ETLImpl extends ETLFileOperations{
  override def inputpath: String = "/home/jatin/IdeaProjects/ETL_Process/InputFiles"
  override def outputpath: String = "/home/jatin/IdeaProjects/ETL_Process/OutputFiles"
  def main(args: Array[String]): Unit = {
    val listfiles=getAllFileWithFormat("")
    println(s"All files List in input directory $listfiles")
    val output=captialiseAllFile(".txt")
    val outputfiles=getAllFileWithFormat(outputpath)
    println(s"List of files in output directory $outputfiles")
    val listofword=listToMap("Testing.txt")
    println("MApping of Data with its count in file")
    listofword.map(pair=> println(pair._1+" -> "+pair._2))

  }
}
