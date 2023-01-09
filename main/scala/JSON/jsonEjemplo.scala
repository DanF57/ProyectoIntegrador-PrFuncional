package JSON
import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._

object jsonEjemplo {
  val jsonValue =
    """
{
  "business":
  {
    "name":"Some Business Name",
    "preferredUrl":"someurl",
    "businessPhone":"somenumber",
    "retailer":
    {
      "firstName":"Some",
      "lastName":"One",
      "email":"someone@somewhere.com",
      "mobileNo":"someothernumber",
      "password":"$^^HFKH*"
    }
  }

}
"""

  def printJson = {

    implicit val rltRds = (
      (__ \ "firstName").read[String] ~
        (__ \ "lastName").read[String] ~
        (__ \ "email").read[String] ~
        (__ \ "mobileNo").read[String] ~
        (__ \ "password").read[String]
      ) //(Retailer)


    implicit val bsnsRds = ({
      val business = (__ \ "business")
      (business \ "name").read[String] ~
        (business \ "preferredUrl").read[String] ~
        (business \ "businessPhone").read[String]
      //  (business \ "retailer").read[Retailer](rltRds)
    }) //(Business)


    val buisness = Json.parse(jsonValue) //.validate[Business](bsnsRds)
    //val json = Json.toJson(bus)

    println(buisness)
  }


  def main(args: Array[String]): Unit = {
    printJson
  }
}
