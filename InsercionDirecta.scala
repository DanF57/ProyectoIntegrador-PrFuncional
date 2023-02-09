
import com.github.tototoshi.csv._
import scalikejdbc._

import java.io.File

import play.api.libs.json._

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

import requests._

object InsercionDirecta extends App {

  //Conexión Base de Datos
  Class.forName("com.mysql.cj.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/bddfinal", "root", "daniel123")
  implicit val session: DBSession = AutoSession

  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()

  //-----------------------------------------------------Entidades-----------------------------------------------------

  //-------------Tabla OriLanguage-------------
  val originalLanguage = data
    .flatMap(elem => elem.get("original_language"))
    .filter(x => x.nonEmpty)
    .distinct

  val originalLanguageTable = originalLanguage.map(x =>
    sql"""
        INSERT INTO original_language(name_original_language)
        VALUES
        (${x})
        """.stripMargin
      .update
      .apply())

  //-------------Tabla Movies-------------
  case class Movie(idMovie: Int,
                   indedx: Int,
                    budget: Long,
                    homepage: String,
                    keywords: String,
                   original_language: String,
                    original_title: String,
                    overview: String,
                    popularity: Double,
                    release_date: String,
                    revenue: Long,
                    runtime: Double,
                    idStatus: Int,
                    tagline: String,
                    title: String,
                    vote_average: Double,
                    vote_count: Int
                   )

  val movieData = data
    .map(row => Movie(
      row("id").toInt,
      row("index").toInt,
      row("budget") match {
        case valueOfRT if valueOfRT.trim.isEmpty => 0
        case valueOfRT => valueOfRT.toLong
      },
      row("homepage"),
      row("keywords"),
      row("original_language"),
      row("original_title"),
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
      row("status") match {
        case status if status == "Released" => 1
        case status if status == "Post Production" => 2
        case status if status == "Rumored" => 3
      },
      row("tagline"),
      row("title"),
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
      INSERT INTO Movie(idMovie, budget, homepage, keywords, idOrigLang, original_title, overview, popularity,
      release_date, revenue, runtime, idStatus, tagline, title, vote_average, vote_count)
      VALUES
      (${x.idMovie}, ${x.budget}, ${x.homepage},${x.keywords}, ${x.original_language}, ${x.original_title}, ${x.overview},
      ${x.popularity}, ${x.release_date}, ${x.revenue}, ${x.runtime},${x.idStatus}, ${x.tagline},
      ${x.title}, ${x.vote_average}, ${x.vote_count})
    """.stripMargin
      .update
      .apply())

  //-------------Tabla Genres-------------
  val genresData = data
    .flatMap(elem => elem.get("genres"))
    .map(x => x.replace("Science Fiction", "Science-Fiction"))
    .filter(x => x.nonEmpty)
    .flatMap(x => x.split(" "))
    .distinct

  val TableGenres = genresData.map(x =>
    sql"""
        INSERT INTO Genre(nameGenre)
        VALUES
        (${x})
        """.stripMargin
      .update
      .apply())

  //-----------------------------------------------------Relaciones-----------------------------------------------------
  //-------------Tabla Movie_Genres------------
  val movieGenres = data
    .map(row => (row("id"), row("genres")))
    .map(x => (x._1, x._2.replace("Science Fiction", "Science-Fiction")))
    .filter(_._2.nonEmpty)
    .map(x => (x._1.toInt, x._2.split(" ").toList))
    .flatMap(x => x._2.map((x._1, _)))

  val movie_genres = movieGenres.map(x =>
    sql"""
     INSERT INTO Movie_genres(idMovie, idGenre)
     VALUES
     (${x._1}, ${x._2})
    """.stripMargin
      .update
      .apply())

  //-------------Tabla Movie_Companies-------------
  val movieCompanies = data
    .map(row => (row("id"), Json.parse(row("production_companies"))))
    .map(row => (row._1, (row._2 \\ "id").toList))
    .flatMap(x => x._2.map((x._1, _)))
    .map(x => (x._1.toInt, x._2.as[Int]))

  val movie_Companies = movieCompanies.map(x =>
    sql"""
         INSERT INTO Movie_production_companies(idMovie, pCompanyId)
         VALUES
         (${x._1}, ${x._2})
         """.stripMargin
      .update
      .apply())

  //-------------Tabla Movie_Countires------------
  val movieCountries = data
    .map(row => (row("id"), Json.parse(row("production_countries"))))
    .map(row => (row._1.toInt, (row._2 \\ "iso_3166_1").toList))
    .flatMap(x => x._2.map((x._1, _)))
    .map(x => (x._1, x._2.as[String]))

  val movies_Countries = movieCountries.map(x =>
    sql"""
           INSERT INTO Movie_production_countries(idMovie, iso_3166_1)
           VALUES
           (${x._1}, ${x._2})
           """.stripMargin
      .update
      .apply())


  //-------------Tabla Movie_Languages------------
  val movieLanguages = data
    .map(row => (row("id"), Json.parse(row("spoken_languages"))))
    .map(row => (row._1, (row._2 \\ "iso_639_1").toList))
    .flatMap(x => x._2.map((x._1, _)))
    .map(x => (x._1.toInt, x._2.as[String]))

  val movies_Languages = movieLanguages.map(x =>
    sql"""
             INSERT INTO Movie_spoken_languages(idMovie, iso_639_1)
             VALUES
             (${x._1}, ${x._2})
             """.stripMargin
      .update
      .apply())

  //-------------------------------------------Funciones de Limpieza----------------------------------------------
  def replacePattern(original: String): String = {
    var txtOr = original

    val pattern: Regex = "(:\\s'\"(.?)',)|([a-z]\\s\"(.?)\"\\s*[A-Z])|(\\s\"(.*?)\",)".r
    for (m <- pattern.findAllIn(original)) {
      val textOriginal = m
      val replacementText = m.replace("\"", "-u0022").replace("'", "-u0027")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }
    txtOr
  }
}
