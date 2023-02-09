
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
  /*
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
                    idOriginalLanguage: Int,
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
      row("original_language") match {
        case x if x == "en" => 1
        case x if x == "ja" => 2
        case x if x == "fr" => 3
        case x if x == "zh" => 4
        case x if x == "es" => 5
        case x if x == "de" => 6
        case x if x == "hi" => 7
        case x if x == "ru" => 8
        case x if x == "ko" => 9
        case x if x == "te" => 10
        case x if x == "cn" => 11
        case x if x == "it" => 12
        case x if x == "nl" => 13
        case x if x == "ta" => 14
        case x if x == "sv" => 15
        case x if x == "th" => 16
        case x if x == "da" => 17
        case x if x == "xx" => 18
        case x if x == "hu" => 19
        case x if x == "cs" => 20
        case x if x == "pt" => 21
        case x if x == "is" => 22
        case x if x == "tr" => 23
        case x if x == "nb" => 24
        case x if x == "af" => 25
        case x if x == "pl" => 26
        case x if x == "he" => 27
        case x if x == "ar" => 28
        case x if x == "vi" => 29
        case x if x == "ky" => 30
        case x if x == "id" => 31
        case x if x == "ro" => 32
        case x if x == "fa" => 33
        case x if x == "no" => 34
        case x if x == "sl" => 35
        case x if x == "ps" => 36
        case x if x == "el" => 37
      },
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
      INSERT INTO Movie(idMovie, `index`, budget, homepage, keywords, idOrigLang, original_title, overview, popularity,
      release_date, revenue, runtime, idStatus, tagline, title, vote_average, vote_count)
      VALUES
      (${x.idMovie}, ${x.indedx}, ${x.budget}, ${x.homepage},${x.keywords}, ${x.idOriginalLanguage}, ${x.original_title}, ${x.overview},
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
    .map(x => (x._1, x._2 match {
      case x if x == "Action" => 1
      case x if x == "Adventure" => 2
      case x if x == "Fantasy" => 3
      case x if x == "Science-Fiction" => 4
      case x if x == "Crime" => 5
      case x if x == "Drama" => 6
      case x if x == "Thriller" => 7
      case x if x == "Animation" => 8
      case x if x == "Family" => 9
      case x if x == "Western" => 10
      case x if x == "Comedy" => 11
      case x if x == "Romance" => 12
      case x if x == "Horror" => 13
      case x if x == "Mystery" => 14
      case x if x == "History" => 15
      case x if x == "War" => 16
      case x if x == "Music" => 17
      case x if x == "Documentary" => 18
      case x if x == "Foreign" => 19
      case x if x == "TV" => 20
      case x if x == "Movie" => 21
    }))

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

*/
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

  def names(dataRaw: String): Option[String] = {
    val response: Response = requests
      .post("http://api.meaningcloud.com/topics-2.0",
        data = Map("key" -> "903292c5e69738082bea48a1b22c3865",
          "lang" -> "en",
          "txt" -> dataRaw,
          "tt" -> "e"),
        headers = Map("content-type" -> "application/x-www-form-urlencoded"))
    Thread.sleep(500)
    if (response.statusCode == 200) {
      Option(response.text)
    } else
      Option.empty
  }
}
