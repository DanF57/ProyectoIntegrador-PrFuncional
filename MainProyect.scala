
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
  INSERT INTO movies(id, index_movie, budget, director, ori_title, title, ori_language, homepage, overview, popularity,
  release_date, revenue, runtime, tagline, vote_average, vote_count)
  VALUES
  (${x.id}, ${x.index_movie}, ${x.budget}, ${x.director}, ${x.ori_title}, ${x.title},${x.ori_language}, ${x.homepage},
  ${x.overview}, ${x.popularity}, ${x.release_date}, ${x.revenue}, ${x.runtime}, ${x.tagline}, ${x.vote_average}, ${x.vote_count})
  """.stripMargin
      .update
      .apply())

  */

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

}
