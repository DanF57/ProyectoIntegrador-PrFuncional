import com.github.tototoshi.csv._
import java.io.File

object Main extends App{
  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()
  //Columnas Numéricas
  val presupuestos = data.flatMap(elem => elem.get("budget")).map(_.toDouble)
  println("Presupuestos")
  estadisticasBasicas(presupuestos)

  val id = data.flatMap(elem => elem.get("id")).map(_.toDouble)
  println("ID")
  estadisticasBasicas(id)

  val popularity = data.flatMap(elem => elem.get("popularity")).map(_.toDouble)
  println("Popularity")
  estadisticasBasicas(popularity)

  //val release_date = data.flatMap(elem => elem.get("release_date")).map(_.toDouble)

  val revenue = data.flatMap(elem => elem.get("revenue")).map(_.toDouble)
  println("Revenue")
  estadisticasBasicas(revenue)

  //val runtime = data.flatMap(elem => elem.get("runtime")).map(_.toDouble)

  val vote_average = data.flatMap(elem => elem.get("vote_average")).map(_.toDouble)
  println("Vote Average")
  estadisticasBasicas(vote_average)

  val vote_count = data.flatMap(elem => elem.get("vote_count")).map(_.toDouble)
  println("Vote Count")
  estadisticasBasicas(vote_count)


  //Columnas Tipo Texto
  val genres = data.flatMap(elem => elem.get("genres"))
  println("Genres")
  frecuencia(genres)

  val homepage = data.flatMap(elem => elem.get("homepage"))
  println("Homepage")
  frecuencia(homepage)

  val keywords = data.flatMap(elem => elem.get("keywords"))
  println("Keywords")
  frecuencia(keywords)

  val original_language = data.flatMap(elem => elem.get("original_language")) //s
  println("Original Language")
  frecuencia(original_language)

  val original_title = data.flatMap(elem => elem.get("original_title"))//s
  println("Original Title")
  frecuencia(original_title)

  val overview = data.flatMap(elem => elem.get("overview"))
  println("Overview")
  frecuencia(overview)

  val status = data.flatMap(elem => elem.get("status"))
  println("Status")
  frecuencia(status)

  val tagline = data.flatMap(elem => elem.get("tagline"))
  println("Tagline")
  frecuencia(tagline)

  val title = data.flatMap(elem => elem.get("title"))
  println("Title")
  frecuencia(title)

  val cast = data.flatMap(elem => elem.get("cast"))
  println("Cast")
  frecuencia(cast)

  val director = data.flatMap(elem => elem.get("director"))
  println("Director")
  frecuencia(director)

  def estadisticasBasicas(columna: List[Double]){
    //Max
    println("Valor Máximo: " + columna.max)
    //Min sin 0s
    println("Valor Mínimo sin ceros: " + columna.filter(_ > 0).min)
    //Promedio con 0s
    val preAvg0 = average(columna)
    println("Promedio con ceros: " + preAvg0)
    //Promedio sin 0s
    val presupuestoAvg = average(columna.filter(_ > 0))
    println("Promedio sin ceros: " + presupuestoAvg)
  }

  def frecuencia(columna: List[String]){
    println(columna.groupBy(x => x).map(t => (t._1, t._2.length)).toList.sortBy(_._2)(Ordering[Int].reverse))
  }

  def average(valores: List[Double]): Double = {
    val t = valores.foldLeft((0.0, 0))((acc, currVal) => (acc._1 + currVal, acc._2 + 1))
    t._1 / t._2
  }
}
