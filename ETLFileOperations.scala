import java.io.{File, FileNotFoundException, IOException, PrintWriter}
import java.nio.file.{Files,Paths}
import scala.io.Source


/**
  * Created by knoldus on 30/1/17.
  */
trait ETLFileOperations extends DBOperation with ConvertToUpperCase{
  def inputpath:String
  def outputpath:String

  private def fileDataToUpper(filename:String ):String={
    val writer = new PrintWriter(new File(outputpath+"/"+filename))
    println(s"File to be modified : ${inputpath+"/"+filename}")

      val upperdata=getData(filename).toUpperCase()
      writer.write(upperdata)


      writer.close()

    outputpath+"/"+filename
  }
  def captialiseAllFile(format:String): String =
  {
    val listoffiles=getAllFileWithFormat(inputpath,format)
    if(listoffiles.size==0)
      println("No Files To Be Modified")
    val outputdata=listoffiles.map(fileDataToUpper(_))
    outputpath
  }

  def getAllFileWithFormat(path:String,format:String ) ={
    if(Files.exists(Paths.get(path))){
      val filesHere = (new java.io.File(path)).listFiles
      val outputfile= for (file <- filesHere if file.getName.endsWith(format)) yield(file.getName)
      outputfile.toList
    }
   else
      {
        println("Path doesnot exist check input or output path Programm exited ")
        sys.exit(1)
      }
  }


  def getData(filename:String):String ={
    val test=inputpath + "/" + filename
    if(Files.exists(Paths.get(test))){
      val str=Source.fromFile(inputpath + "/" + filename).mkString
      str
    }
    else
      {
        println("No such File or directory")
        ""
      }
  }
  def printData(filename:String)={
    Source.fromFile(filename).foreach {
      print
    }
  }
  def uniqueWordToMap(filename:String):Map[String,Int] = {
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
