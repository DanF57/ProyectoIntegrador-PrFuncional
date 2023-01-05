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
"""

  def printJson = {

    implicit val rltRds = (
      (__ \ "director").read[String] ~
        (__ \ "frs_actor").read[String] ~
        (__ \ "sec_actor").read[String]
      ) //(Retailer)


    implicit val bsnsRds = ({
      val business = (__ \ "movies")
      (business \ "name").read[String] ~
        (business \ "original_title").read[String] ~
        (business \"genres").read[String]~
        (business \ "relaseDate").read[String]

    })


    val movies = Json.parse(jsonValue)

    println(movies)
  }


  def main(args: Array[String]): Unit = {
    printJson
  }
}
