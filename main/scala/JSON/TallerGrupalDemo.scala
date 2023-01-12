import play.api.libs.json._

import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex
import com.github.tototoshi.csv._

import java.io.File

object TallerGrupalDemo extends App {

  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()

  def replacePattern(original: String): String = {
    var txtOr = original

    val pattern3: Regex = "(:\\s'\"(.*?)',)".r
    for (m <- pattern3.findAllIn(original)) {
      val textOriginal = m
      val replacementText = m.replace("\"", "-u0022")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }

    val pattern2: Regex = "([a-z]\\s\"(.*?)\"\\s*[A-Z])".r
    for (m <- pattern2.findAllIn(txtOr)) {
      val textOriginal = m
      val replacementText = m.replace("\"", "-u0022")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }

    val pattern1: Regex = "(\\s\"(.*?)\",)".r
    for (m <- pattern1.findAllIn(txtOr)) {
      val textOriginal = m
      val replacementText = m.replace("'", "-u0027")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }
    txtOr
  }

    val crew = data
      .map(row => row("crew"))
      .map(replacePattern)
      .map(text => text.replace("'", "\""))
      .map(text => text.replace("-u0027", "'"))
      .map(text => text.replace("-u0022", "\\\""))
      .map(text => Try(Json.parse(text)))
      .filter(_.isSuccess)

    //Departamentos Megamind
    val megamindDeps = crew(196).map(jsonData => jsonData \\ "department").map(_.toList).get.distinct
    println("Departamentos de Megamind")
    println(megamindDeps)

    //Trabajos Megamind
    val megamindJobs = crew(196).map(jsonData => jsonData \\ "job").map(_.toList).get.distinct
    println("Jobs de Megamind")
    println(megamindJobs)

    //Departamentos totales
    val departamentos = crew.map(_.get).flatMap(jsonData => jsonData \\ "department").distinct
    println("Departamentos Totales")
    println(departamentos)

    //Jobs Totales
    val jobs = crew.map(_.get).flatMap(jsonData => jsonData \\ "job").distinct
    println("Jobs Totales")
    println(jobs)

    val departamentosGroups = departamentos.groupBy {
      case x => x
    }.map {
      case depar => (depar._1, depar._2.size)
    }.toList
}

/*
val megamindJobs = crew(196).map(jsonData => jsonData \\ "job").map(_.toList).get.groupBy{
  case x => x
}.map{
  case x => (x._1, x._2.size)
  }.toList.orde
 */