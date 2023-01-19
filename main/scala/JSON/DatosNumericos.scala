import com.github.tototoshi.csv._
import java.io.File

object DatosNumericos extends App{
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

  val revenue = data.flatMap(elem => elem.get("revenue")).map(_.toDouble)
  println("Revenue")
  estadisticasBasicas(revenue)

  val runtime = data.flatMap(elem => elem.get("runtime")).filter(x => x != "").map(_.toDouble)
  println("RunTime")
  estadisticasBasicas(runtime)

  val vote_average = data.flatMap(elem => elem.get("vote_average")).map(_.toDouble)
  println("Vote Average")
  estadisticasBasicas(vote_average)

  val vote_count = data.flatMap(elem => elem.get("vote_count")).map(_.toDouble)
  println("Vote Count")
  estadisticasBasicas(vote_count)


  def estadisticasBasicas(columna: List[Double]) {
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

  def average(valores: List[Double]): Double = {
    val t = valores.foldLeft((0.0, 0))((acc, currVal) => (acc._1 + currVal, acc._2 + 1))
    t._1 / t._2
  }
}
