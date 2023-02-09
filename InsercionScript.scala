
import com.github.tototoshi.csv._
import java.io.File
import play.api.libs.json._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import requests._

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}


object InsercionScript extends App {

  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()


  //--------------------------------------- GENRES ENTIDAD ---------------------------------------------------------
  val genresData = data
    .flatMap(elem => elem.get("genres"))
    .map(x => x.replace("Science Fiction", "Science-Fiction"))
    .filter(x => x.nonEmpty)
    .flatMap(x => x.split(" "))
    .distinct

  val SQL_INSERT_PATTERN_GENRES =
    """INSERT INTO Genres (genre_name)
      |VALUES
      |('%s');
      |""".stripMargin

  val scriptDataGenres = genresData
    .map(genre => SQL_INSERT_PATTERN_GENRES.formatLocal(java.util.Locale.US,
      genre))

  val scriptFileGenres = new File("C:\\Users\\Daniel\\Utpl/genresInsert.sql")
  if (scriptFileGenres.exists()) scriptFileGenres.delete()

  scriptDataGenres.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\Daniel\\Utpl/genresInsert.sql"),
      insert.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND)
  )

  //--------------------------------------- GENRES MOVIES RELACION --------------------------------------------------
  val genresMovies = data
    .map(row => (row("id"), row("genres")))
    .map(x => (x._1.toInt, x._2.replace("Science Fiction", "Science-Fiction")))
    .filter(_._2.nonEmpty)
    .map(x => (x._1, x._2.split(" ").toList))
    .flatMap(x => x._2.map((x._1, _)))

  val SQL_INSERT_GENRES_MOVIES =
    """INSERT INTO Movie_genres (idMovie, genre_name)
      |VALUES
      |(%d, '%s');
      |""".stripMargin

  val scriptDataGenresMovies = genresMovies
    .map(genre => SQL_INSERT_GENRES_MOVIES.formatLocal(java.util.Locale.US,
      genre._1,
      genre._2))

  val scriptFile_movies_genres = new File("C:\\Users\\Daniel\\Utpl/genresMoviesInsert.sql")
  if (scriptFile_movies_genres.exists()) scriptFile_movies_genres.delete()

  scriptDataGenresMovies.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\Daniel\\Utpl/genresMoviesInsert.sql"),
      insert.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND)
  )

  //--------------------------------------- Personas ENTIDAD ---------------------------------------------------------
  val castData = data
    .map(row => row("cast"))
    .filter(_.nonEmpty)
    .take(1000)
    .map(StringContext.processEscapes)
    .map(names)
    .map(json => Try(Json.parse(json.get)))
    .filter(_.isSuccess)
    .map(_.get)
    .flatMap(json => json("entity_list").as[JsArray].value)
    .map(_("form"))
    .map(data => data.as[String])
    .distinct

  val SQL_INSERT_CAST =
    """INSERT INTO `cast`(actor_name)
      |VALUES
      |('%s');
      |""".stripMargin

  val scriptDataCast = castData
    .map(castName => SQL_INSERT_CAST.format(escapeMysql(castName)))

  val scriptFile_cast = new File("C:\\Users\\Daniel\\Utpl/castInsert.sql")
  if (scriptFile_cast.exists()) scriptFile_cast.delete()

  scriptDataCast.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\Daniel\\Utpl/castInsert.sql"),
      insert.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND)
  )

  //--------------------------------------- CAST MOVIE RELACION ------------------------------------------------------
  val moviesCast = data
    .map(row => (row("id"), castData))
    .filter(_._2.nonEmpty)
    .map(x => (x._1, x._2))
    .flatMap(x => x._2.map((x._1.toInt, _)))

  val SQL_INSERT_MOVIES_CAST =
    """INSERT INTO movies_cast(id, actor_name)
      |VALUES
      |(%d, '%s');
      |""".stripMargin

  val scriptDataCastMovies = moviesCast
    .map(x => SQL_INSERT_MOVIES_CAST.formatLocal(java.util.Locale.US,
      x._1,
      escapeMysql(x._2)))

  val scriptFile_movies_cast = new File("C:\\Users\\Daniel\\Utpl/castMoviesInsert.sql")
  if (scriptFile_movies_cast.exists()) scriptFile_movies_cast.delete()

  scriptDataCastMovies.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\Daniel\\Utpl/castMoviesInsert.sql"),
      insert.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND)
  )

  def names(dataRaw: String): Option[String] = {
    val response: Response = requests
      .post("http://api.meaningcloud.com/topics-2.0",
        data = Map("key" -> "6104b7e01397e5c31dfa9593300b5d26",
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
