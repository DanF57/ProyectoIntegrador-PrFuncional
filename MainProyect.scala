
import com.github.tototoshi.csv._
import scalikejdbc._

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import play.api.libs.json._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object MainProyect extends App {

  //Conexión Base de Datos
  Class.forName("com.mysql.cj.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/proyecto_integrador", "root", "daniel123")
  implicit val session: DBSession = AutoSession

  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()

  /*
  case class Movies(id: Int,
                    index_movie: Int,
                    budget: Long,
                    director: String,
                    ori_title: String,
                    title: String,
                    ori_language: String,
                    keywords: String,
                    homepage: String,
                    overview: String,
                    popularity: Double,
                    release_date: String,
                    revenue: Long,
                    runtime: Double,
                    tagline: String,
                    vote_average: Double,
                    vote_count: Int
                   )

  val movieData = data
    .map(row => Movies(
      row("id").toInt,
      row("index") match {
        case valueOfRT if valueOfRT.trim.isEmpty => 0
        case valueOfRT => valueOfRT.toInt
      },
      row("budget") match {
        case valueOfRT if valueOfRT.trim.isEmpty => 0
        case valueOfRT => valueOfRT.toLong
      },
      row("director"),
      escapeMysql(row("original_title")),
      row("title"),
      row("original_language"),
      row("keywords"),
      row("homepage"),
      row("overview"),
      row("popularity") match {
        case valueOfRT if valueOfRT.trim.isEmpty => 0.0
        case valueOfRT => valueOfRT.toDouble
      },
      row("release_date") match {
        case valueOfRT if valueOfRT.trim.isEmpty => null
        case valueOfRT => valueOfRT
      },
      row("revenue") match {
        case valueOfRT if valueOfRT.trim.isEmpty => 0
        case valueOfRT => valueOfRT.toLong
      },
      row("runtime") match {
        case valueOfRT if valueOfRT.trim.isEmpty => 0.0
        case valueOfRT => valueOfRT.toDouble
      },
      row("tagline"),
      row("vote_average") match {
        case valueOfRT if valueOfRT.trim.isEmpty => 0.0
        case valueOfRT => valueOfRT.toDouble
      },
      row("vote_count") match {
        case valueOfRT if valueOfRT.trim.isEmpty => 0
        case valueOfRT => valueOfRT.toInt
      }))

  val movies = movieData.map(x =>
    sql"""
  INSERT INTO movies(id, index_movie, budget, director, ori_title, title, ori_language, keywords, homepage,
  overview, popularity, release_date, revenue, runtime, tagline, vote_average, vote_count)
  VALUES
  (${x.id}, ${x.index_movie}, ${x.budget}, ${x.director}, ${x.ori_title}, ${x.title}, ${x.ori_language}, ${x.keywords},
  ${x.homepage}, ${x.overview}, ${x.popularity}, ${x.release_date}, ${x.revenue}, ${x.runtime}, ${x.tagline},
  ${x.vote_average}, ${x.vote_count})
  """.stripMargin
      .update
      .apply())

  val companiesData = data
    .flatMap(row => row.get("production_companies"))
    .map(Json.parse)
    .flatMap(x => x.as[List[JsValue]])
    .map(x => (x("name").as[String], x("id").as[Int]))
    .toSet

  val movies = companiesData.map(x =>
    sql"""
    INSERT INTO production_companies(comp_id, comp_name)
    VALUES
    (${x._2}, ${x._1})
    """.stripMargin
      .update
      .apply())

  val countriesData = data
    .flatMap(row => row.get("production_countries"))
    .map(Json.parse)
    .flatMap(x => x.as[List[JsValue]])
    .map(x => (x("iso_3166_1").as[String], x("name").as[String]))
    .distinct
    .sortBy(_._2)

  val countries = countriesData.map(x =>
    sql"""
      INSERT INTO production_countries(prod_iso_cod, countr_name)
      VALUES
      (${x._1}, ${x._2})
      """.stripMargin
      .update
      .apply())


  val spokenData = data
    .flatMap(row => row.get("spoken_languages"))
    .map(Json.parse)
    .flatMap(x => x.as[List[JsValue]])
    .map(x => (x("iso_639_1").as[String], x("name").as[String]))
    .toSet

  val spokenLanguage = spokenData.map(x =>
    sql"""
        INSERT INTO spoken_language(lang_iso_cod, lang_name)
        VALUES
        (${x._1}, ${x._2})
        """.stripMargin
      .update
      .apply())



  case class Crew(crew_name: String,
                  gender: Int,
                  department: String,
                  job: String,
                  credit_id: String,
                  personal_id: Int
                 )

  val crewData = data
    .flatMap(row => row.get("crew"))
    .map(replacePattern)
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))
    .map(StringContext.processEscapes)
    .map(text => Try(Json.parse(text)))
    .filter(_.isSuccess)
    .map(_.get)
    .flatMap(x => x.as[List[JsValue]])
    .map(row => Crew(row("name").as[String],
      row("gender").as[Int],
      row("department").as[String],
      row("job").as[String],
      row("credit_id").as[String],
      row("id").as[Int]))

  val crews = crewData.map(x =>
    sql"""
          INSERT INTO crew(crew_name, gender, department, job, credit_id, personal_id)
          VALUES
          (${x.crew_name}, ${x.gender}, ${x.department}, ${x.job}, ${x.credit_id}, ${x.personal_id})
          """.stripMargin
      .update
      .apply())


  val statusData = data.flatMap(elem => elem.get("status")).distinct
  val crews = statusData.map(x =>
    sql"""
            INSERT INTO status(status_name)
            VALUES
            (${x})
            """.stripMargin
      .update
      .apply())


  val genresData = data
    .flatMap(elem => elem.get("genres"))
    .map(x => x.replace("Science Fiction", "Science-Fiction"))
    .filter(x => x.nonEmpty)
    .flatMap(x => x.split(" "))
    .distinct

  val genres = genresData.map(x =>
    sql"""
       INSERT INTO genres(genre_name)
       VALUES
       (${x})
       """.stripMargin
      .update
      .apply())
   */

  def escapeMysql(text: String): String = text
    .replaceAll("\\\\", "\\\\\\\\")
    .replaceAll("\b", "\\\\b")
    .replaceAll("\n", "\\\\n")
    .replaceAll("\r", "\\\\r")
    .replaceAll("\t", "\\\\t")
    .replaceAll("\\x1A", "\\\\Z")
    .replaceAll("\\x00", "\\\\0")
    .replaceAll("'", "\\\\'")
    .replaceAll("\"", "\\\\\"")

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

}
