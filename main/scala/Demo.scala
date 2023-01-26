import play.api.libs.json._

import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex
import com.github.tototoshi.csv._

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Demo extends App {

  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()

  val release_date = data.flatMap(elem => elem.get("release_date"))
  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val releaseDateList = data
    .map(row => row("release_date"))
    .filter(!_.equals(""))
    .map(text => LocalDate.parse(text, dateFormatter))
  val yearReleaseList = releaseDateList
    .map(_.getYear)

  val yearGroup = yearReleaseList.groupBy {
    case x => x
  }.map {
    case depar => (depar._1.toString(), depar._2.size)
  }
    .toList
    .sortBy(_._2)
    .reverse

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

  val idNameList = crew
    .map(_.get)
    .flatMap(_.as[JsArray].value)
    .map(_.as[JsObject])
    .map(jsObj => (jsObj("id").as[Int], jsObj("name").as[String]))
    .groupBy(identity)
    .map {
      case (tupla, list) => (tupla, list.size)
    }
    .filter(_._2 > 1)
    .toList
    .sortBy(_._2)
    .reverse

    println(yearGroup)

}