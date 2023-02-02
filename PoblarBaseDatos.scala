
import com.github.tototoshi.csv._
import scalikejdbc._

import java.io.File

import play.api.libs.json._

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

import requests._

object PoblarBaseDatos extends App {

  //Conexión Base de Datos
  Class.forName("com.mysql.cj.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/proyecto_integrador", "root", "daniel123")
  implicit val session: DBSession = AutoSession

  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()

  //-------------Tabla Directores-------------
  val directorData = data.flatMap(elem => elem.get("director")).distinct
  val directors = directorData.map(x =>
    sql"""
      INSERT INTO director(director_name)
      VALUES
      (${StringContext.processEscapes(x)})
    """.stripMargin
      .update
      .apply())

  //-------------Tabla Status-------------
  val statusData = data.flatMap(elem => elem.get("status")).distinct
  val statuss = statusData.map(x =>
    sql"""
      INSERT INTO status(status_name)
      VALUES
      (${x})
    """.stripMargin
      .update
      .apply())

  //-----------------------------------------------------Entidades-----------------------------------------------------
  //-------------Tabla Movies-------------
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
                    status_name: String,
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
      StringContext.processEscapes(row("director")),
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
      row("status"),
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
      INSERT INTO movies(id, index_movie, budget, director_name, ori_title, title, ori_language, keywords, homepage,
      overview, popularity, status_name, release_date, revenue, runtime, tagline, vote_average, vote_count)
      VALUES
      (${x.id}, ${x.index_movie}, ${x.budget}, ${x.director},${x.ori_title}, ${x.title}, ${x.ori_language}, ${x.keywords},
      ${x.homepage}, ${x.overview}, ${x.popularity}, ${x.status_name},${x.release_date}, ${x.revenue},
      ${x.runtime}, ${x.tagline}, ${x.vote_average}, ${x.vote_count})
    """.stripMargin
      .update
      .apply())

  //-------------Tabla Production Companies-------------
  val companiesData = data
    .flatMap(row => row.get("production_companies"))
    .map(Json.parse)
    .flatMap(x => x.as[List[JsValue]])
    .map(x => (x("name").as[String], x("id").as[Int]))
    .toSet

  val companies = companiesData.map(x =>
    sql"""
    INSERT INTO production_companies(comp_id, comp_name)
    VALUES
    (${x._2}, ${x._1})
    """.stripMargin
      .update
      .apply())

  //-------------Tabla Production Countries-------------
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

  //-------------Tabla Spoken Language-------------
  val spokenData = data
    .flatMap(row => row.get("spoken_languages"))
    .map(Json.parse)
    .flatMap(x => x.as[List[JsValue]])
    .map(x => (x("iso_639_1").as[String], x("name").as[String]))
    .toSet

  val spoken = spokenData.map(x =>
    sql"""
        INSERT INTO spoken_language(lang_iso_cod, lang_name)
        VALUES
        (${x._1}, ${x._2})
        """.stripMargin
      .update
      .apply())

  //-------------Tabla Crew-------------
  case class Crew(crew_name: String,
                  gender: Int,
                  department: String,
                  job: String,
                  credit_id: String,
                  personal_id: Int
                 )

  val crewData = data
    .flatMap(row => row.get("crew"))
    .filter(_.nonEmpty)
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

  val crew = crewData.map(x =>
    sql"""
      INSERT INTO crew(crew_name, gender, department, job, credit_id, personal_id)
      VALUES
      (${x.crew_name}, ${x.gender}, ${x.department}, ${x.job}, ${x.credit_id}, ${x.personal_id})
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

  val genres = genresData.map(x =>
    sql"""
       INSERT INTO genres(genre_name)
       VALUES
       (${x})
       """.stripMargin
      .update
      .apply())

  //-------------Tabla Cast-------------
  val castData = data
    .map((row) => row("cast"))
    .filter(_.nonEmpty)
    .take(100)
    .map(StringContext.processEscapes)
    .map(names)
    .map(json => Try(Json.parse(json.get)))
    .filter(_.isSuccess)
    .map(_.get)
    .flatMap(json => json("entity_list").as[JsArray].value)
    .map(_("form"))
    .map(data => data.as[String])
    .distinct
    .toSet

  val cast = castData.map(x =>
    sql"""
         INSERT INTO `cast`(actor_name)
         VALUES
         (${x})
         """.stripMargin
      .update
      .apply())

  //-----------------------------------------------------Relaciones-----------------------------------------------------
  //-------------Tabla Movie_Companies-------------
  val movieCompanies = data
    .map(row => (row("id"), Json.parse(row("production_companies"))))
    .map(row => (row._1, (row._2 \\ "id").toList))
    .flatMap(x => x._2.map((x._1, _)))
    .map(x => (x._1.toInt, x._2.toString().toInt))

  val movie_Companies = movieCompanies.map(x =>
    sql"""
           INSERT INTO movies_companies(id, comp_id)
           VALUES
           (${x._1}, ${x._2})
           """.stripMargin
      .update
      .apply())

  //-------------Tabla Movie_Countires------------
  val movieCountries = data
    .map(row => (row("id"), Json.parse(row("production_countries"))))
    .map(row => (row._1, (row._2 \\ "iso_3166_1").toList))
    .flatMap(x => x._2.map((x._1, _)))
    .map(x => (x._1.toInt, escapeMysql2(x._2.toString)))

  val movies_Countries = movieCountries.map(x =>
    sql"""
             INSERT INTO movies_countries(id, prod_iso_cod)
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
    .map(x => (x._1.toInt, escapeMysql2(x._2.toString)))

  val movies_Languages = movieLanguages.map(x =>
    sql"""
               INSERT INTO movies_languages(id, lang_iso_cod)
               VALUES
               (${x._1}, ${x._2})
               """.stripMargin
      .update
      .apply())

  //-------------Tabla Movie_Crew------------
  val movieCrew = data
    .map(row => (row("id"), row("crew")))
    .filter(_._2.nonEmpty)
    .map(x => (x._1, replacePattern(x._2)))
    .map(x => (x._1, x._2.replace("'", "\"")))
    .map(x => (x._1, x._2.replace("-u0027", "'")))
    .map(x => (x._1, x._2.replace("-u0022", "\\\"")))
    .map(x => (x._1, StringContext.processEscapes(x._2)))
    .map(x => (x._1, Try(Json.parse(x._2))))
    .filter(_._2.isSuccess)
    .map(x => (x._1, x._2.get))
    .map(row => (row._1, (row._2 \\ "credit_id").toList))
    .flatMap(x => x._2.map((x._1, _)))
    .map(x => (x._1.toInt, (escapeMysql2(x._2.toString))))

  val movie_crews = movieCrew.map(x =>
    sql"""
      INSERT INTO movies_crew(id, credit_id)
      VALUES
      (${x._1}, ${x._2})
      """.stripMargin
      .update
      .apply())

  //-------------Tabla Movie_Genres------------
  val movieGenres = data
    .map(row => (row("id"), row("genres")))
    .map(x => (x._1, x._2.replace("Science Fiction", "Science-Fiction")))
    .filter(_._2.nonEmpty)
    .map(x => (x._1, x._2.split(" ").toList))
    .map(x => x._2.map((x._1, _)))

  val movie_genres = movieGenres.map(_.map(x =>
    sql"""
       INSERT INTO movies_genres(id, genre_name)
       VALUES
       (${x._1}, ${x._2})
       """.stripMargin
      .update
      .apply()))

  //-------------Tabla Movie_Cast------------
  val moviesCast = data
    .map(row => (row("id"), row("cast")))
    .filter(_._2.nonEmpty)
    .map(x => (x._1, StringContext.processEscapes(x._2)))
    .take(100) //Reestriccion
    .map(x => (x._1, names(x._2)))
    .map(x => (x._1, Try(Json.parse(x._2.get))))
    .filter(_._2.isSuccess)
    .map(x => (x._1, x._2.get))
    .map(x => (x._1, x._2("entity_list").as[JsArray].value))
    .map(x => (x._1, x._2.map(_("form"))))
    .map(x => (x._1.toInt, x._2.map(_.toString()).toList))
    .map(x => x._2.map((x._1, _)))

  val movie_cast = moviesCast.map(_.map(x =>
    sql"""
     INSERT INTO movies_cast(id, actor_name)
     VALUES
     (${x._1}, ${escapeMysql2(x._2)})
     """.stripMargin
      .update
      .apply()))


  //-------------------------------------------Funciones de Limpieza----------------------------------------------
  def escapeMysql2(text: String): String = text
    .replaceAll("\"", "")

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
