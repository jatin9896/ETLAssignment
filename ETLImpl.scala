/**
  * Created by knoldus on 30/1/17.
  */
import java.io.{File, FileNotFoundException, IOException, PrintWriter}

import scala.io.Source
/**
  * Created by jatin on 28/1/17.
  */


object ETLImpl extends ETLFileOperations{
  override def inputpath: String = "/home/knoldus/IdeaProjects/ETLImpl"
  override def outputpath: String = "/home/knoldus/IdeaProjects/ETLImpl/output"
  def main(args: Array[String]): Unit = {
    val listfiles=getAllFileWithFormat(inputpath,"")
    println(s"All files List in input directory $listfiles")
    val output=captialiseAllFile(".txt")
    val outputfiles=getAllFileWithFormat(outputpath,"")
    println(s"List of files in output directory $outputfiles")
    val listofword=uniqueWordToMap("Testing.txt")
    println("MApping of Data with its count in file")
    listofword.map(pair=> println(pair._1+" -> "+pair._2))

  }
}