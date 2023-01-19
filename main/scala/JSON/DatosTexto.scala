import com.github.tototoshi.csv._
import java.io.File

object DatosTexto extends App{
  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()

  //Columnas Tipo Texto
/*
  val genres = data.flatMap(elem => elem.get("genres"))
  println("Genres")
  frecuencia(genres)

  val homepage = data.flatMap(elem => elem.get("homepage"))
  println("Homepage")
  frecuencia(homepage)

  val keywords = data.flatMap(elem => elem.get("keywords"))
  println("Keywords")
  frecuencia(keywords)
*/
  val original_language = data.flatMap(elem => elem.get("original_language")) //s
  println("Original Language")
  frecuencia(original_language)
/*
  val original_title = data.flatMap(elem => elem.get("original_title"))//s
  println("Original Title")
  frecuencia(original_title)

  val overview = data.flatMap(elem => elem.get("overview"))
  println("Overview")
  frecuencia(overview)

  val release_date = data.flatMap(elem => elem.get("release_date"))
  println("Release Date")
  frecuencia(release_date)

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
*/
  def frecuencia(columna: List[String]) {
    println(columna.groupBy(x => x).map(t => (t._1, t._2.length)).toList.sortBy(_._2)(Ordering[Int].reverse))
  }
}
