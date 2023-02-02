package JSON
import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._

object jsonEjemplo {
  val jsonValue =
    """
{
  "movies":
  {
    "name":"Original Name",
    "original_title":"title",
    "genres":"type_gen",
    "relaseDate":"date",
    "cast":
    {
      "director":"name_director",
      "frs_actor":"name_actor1",
      "sec_actor":"name_actor2"
    }
  }
}
""".replace("\r", "")

  def printJson = {

    implicit val moviesReads = (
      (__ \ "director").read[String] ~
        (__ \ "frs_actor").read[String] ~
        (__ \ "sec_actor").read[String]
      ) //(movie)


    implicit val castReads = ({
      val movies = (__ \ "movies")
      (movies \ "name").read[String] ~
        (movies \ "original_title").read[String] ~
        (movies \"genres").read[String]~
        (movies \ "relaseDate").read[String]~
        (movies \ "cast").read[String]
    })


    val movies = Json.parse(jsonValue)

    println(movies)
  }


  def main(args: Array[String]): Unit = {
    printJson
  }
}