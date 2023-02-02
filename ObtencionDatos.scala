import scalikejdbc._

object ObtencionDatos extends App {

  Class.forName("com.mysql.cj.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/proyecto_integrador", "root", "daniel123")
  implicit val session: DBSession = AutoSession

  //Actores que su nombre empiece con D
  val actorsDl: List[Map[String, Any]] =
    sql"SELECT * FROM movies_cast WHERE(actor_name LIKE 'D%');"
      .map(_.toMap)
      .list.apply()
  //println(actorsDl)
  //Salida: List(Map(id -> 2268, actor_name -> Dakota Blue Richards), Map(id -> 102382, actor_name -> Dane DeHaan),
  // Map(id -> 206647, actor_name -> Daniel Craig), +...)

  //Compañias que realizaron la película Megamind (id: 196)
  val companiesMegamind: List[Map[String, Any]] =
    sql"SELECT comp_id, id FROM movies_companies WHERE(id = 196);"
      .map(_.toMap)
      .list.apply()
  //println(companiesMegamind)
  //Salida: List(Map(comp_id -> 33, id -> 196), Map(comp_id -> 56, id -> 196), Map(comp_id -> 20448, id -> 196))


  val moviesAbove2000: List[Map[String, Any]] =
    sql"""
    SELECT m.id, m.ori_title, m.ori_language, YEAR(m.release_date) as release_year
    FROM movies m
    WHERE (YEAR(release_date) > 2000);
    """
    .map(_.toMap)
    .list.apply()
  //println(moviesAbove2000)
  //Salida: List(Map(id -> 12, ori_title -> Finding Nemo, ori_language -> en, release_year -> 2003), +...)

  val estadisticasMovies: List[Map[String, Any]] =
    sql"""
      SELECT MAX(revenue), MIN(revenue), AVG(revenue)
      FROM movies
      """
      .map(_.toMap)
      .list.apply()
  //println(estadisticasMovies)
  //List(Map(MAX(revenue) -> 2 787 965 087, MIN(revenue) -> 0, AVG(revenue) -> 82 260 638.6517))

  val crewsG0: List[Map[String, Any]] =
    sql"""
      SELECT m.id, m.ori_title, c.crew_name, c.job
      FROM movies m
            JOIN movies_crew mc ON m.id = mc.id
            JOIN crew c ON c.credit_id = mc.credit_id
      WHERE c.department = 'Art' and c.gender = 0;
      """
      .map(_.toMap)
      .list.apply()
  //println(crewsG0)
  //List(Map(id -> 18, ori_title -> The Fifth Element, crew_name -> Ira Gilford, job -> Art Direction),
  // Map(id -> 18, ori_title -> The Fifth Element, +...)
}

