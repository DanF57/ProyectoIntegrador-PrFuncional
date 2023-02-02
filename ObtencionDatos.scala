import scalikejdbc._

object ObtencionDatos extends App {

  Class.forName("com.mysql.cj.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/proyecto_integrador", "root", "daniel123")
  implicit val session: DBSession = AutoSession

  val actorsDl: List[Map[String, Any]] =
    sql"SELECT * FROM movies_cast WHERE(actor_name LIKE 'D%');"
      .map(_.toMap)
      .list.apply()
  println(actorsDl)

  val hgjh: List[Map[String, Any]] =
    sql"SELECT * FROM movies_cast WHERE(actor_name LIKE 'D%');"
      .map(_.toMap)
      .list.apply()
  println(actorsDl)
}
