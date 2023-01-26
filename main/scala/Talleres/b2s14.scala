package Talleres

import com.github.tototoshi.csv.CSVReader
import requests._
import scala.util.{Failure, Success, Try}
import java.io.File
import play.api.libs.json._
import scalikejdbc._

object b2s14 extends App {

  //Lectura DataSet(csv)
  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()

  //Conexión Bse de Datos
  Class.forName("com.mysql.cj.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/movies20223", "root", "daniel123")
  implicit val session: DBSession = AutoSession

  //Funcion Meaningcloud
  def actorsNames(dataRaw: String): Option[String] = {
    val response: Response = requests
      .post("http://api.meaningcloud.com/topics-2.0",
        data = Map("key" -> "a103b77491e7d2674632370e369cf223",
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

  //Obtención y filtración datos cast
  val cast = data
    .map((row1)=> row1("cast"))
    .filter(_.nonEmpty)
    .map(StringContext.processEscapes)
    .take(10) //Restricción
    .map(actorsNames)
    .map(json => Try(Json.parse(json.get)))
    .filter(_.isSuccess)
    .map(_.get)
    .flatMap(json => json("entity_list").as[JsArray].value)
    .map(_("form"))
    .map(data => data.as[String])
    .toSet
    .toList

  //Sentencias SQL

  //INSERT
  val actors = cast.map(x =>
    sql"""
  INSERT INTO Persona(nombres)
  VALUES
  (${x})
  """.stripMargin
      .update
      .apply())

  //SELECT
  val actorsDl: List[Map[String, Any]] =
    sql"SELECT * FROM Persona p WHERE(nombres LIKE 'D%');"
    .map(_.toMap)
    .list.apply()
  println(actorsDl)

}

