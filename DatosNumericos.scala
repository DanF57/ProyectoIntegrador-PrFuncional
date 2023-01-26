import com.github.tototoshi.csv._

import java.util.Locale
import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import com.cibo.evilplot.plot._
import com.cibo.evilplot.colors.RGB
import com.cibo.evilplot.geometry.{Align, Drawable, Extent, Rect, Text}
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import com.cibo.evilplot.plot.{Bar, BarChart}
import com.cibo.evilplot.plot.renderers.BarRenderer
import requests.RequestAuth.Empty

object DatosNumericos extends App{
  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()

  val release_date = data.flatMap(elem => elem.get("release_date"))
  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val releaseDateList = data
    .map(row => row("release_date"))
    .filter(!_.equals(""))
    .map(text => LocalDate.parse(text, dateFormatter))
  val yearReleaseList = releaseDateList
    .map(_.getYear)


  //Columnas Numéricas
  val presupuestos = numericColumn("budget")
  val popularity = numericColumn("popularity")
  val revenue = numericColumn("revenue")
  val runtime = numericColumn("runtime")
  val vote_average = numericColumn("vote_average")
  val vote_count = numericColumn("vote_count")

    //Maximo Minimo Promedio
    val presupuestoMmP = estadisticasBasicas(presupuestos)
    val popularityMmP = estadisticasBasicas(popularity)
    val revenueMmP = estadisticasBasicas(revenue)
    val runtimeMmP = estadisticasBasicas(runtime)
    val vote_averageMmP = estadisticasBasicas(vote_average)
    val vote_countMmP = estadisticasBasicas(vote_count)
    val years = estadisticasBasicas(yearReleaseList.map(_.toDouble))

  def numericColumn(columna: String): List[Double] ={
    data.flatMap(elem => elem.get(columna)).filter(_.nonEmpty).map(_.toDouble)
  }

  def estadisticasBasicas(columna: List[Double]) : Seq[Double] ={
    //Max
    val maximo = columna.max
    //Min sin 0s
    val minimo = columna.filter(_ > 0).filter(_ > 1).min
    //Promedio sin 0s
    val promedio = average(columna.filter(_ > 0))

    Seq[Double](minimo, maximo, promedio)
  }

  def average(valores: List[Double]): Double = {
    val t = valores.foldLeft((0.0, 0))((acc, currVal) => (acc._1 + currVal, acc._2 + 1))
    t._1 / t._2
  }

  //Graficas
  val labeledByColor = new BarRenderer {
    def render(plot: Plot, extent: Extent, category: Bar): Drawable = {
      val rect = Rect(extent)
      val value = category.values.head
      val color = RGB(0, 204, 204)
      Align.center(rect filled color, Text(s"$value", size = 10)
      ).group
    }
  }

  val labelsvA = Seq("Minimo", "Maximo", "Promedio")

  BarChart
    .custom(presupuestoMmP.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
    .title("Presupuestos MmP")
    .standard(xLabels = labelsvA)
    .render()
    .write(new File
    ("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\Practicum\\Gráficas\\Datos Tipo Numérico/Presupuestos.png"))

  BarChart
    .custom(popularityMmP.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
    .title("Popularity MmP")
    .standard(xLabels = labelsvA)
    .render()
    .write(new File
    ("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\Practicum\\Gráficas\\Datos Tipo Numérico/Popularity.png"))

  BarChart
    .custom(revenueMmP.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
    .title("Revenue MmP")
    .standard(xLabels = labelsvA)
    .render()
    .write(new File
    ("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\Practicum\\Gráficas\\Datos Tipo Numérico/Revenue.png"))

  BarChart
    .custom(runtimeMmP.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
    .title("Runtime MmP")
    .standard(xLabels = labelsvA)
    .render()
    .write(new File
    ("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\Practicum\\Gráficas\\Datos Tipo Numérico/Runtime.png"))

  BarChart
    .custom(vote_averageMmP.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
    .title("VoteAvg MmP")
    .standard(xLabels = labelsvA)
    .render()
    .write(new File
    ("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\Practicum\\Gráficas\\Datos Tipo Numérico/VoteAvg.png"))

  BarChart
    .custom(vote_countMmP.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
    .title("VoteCount MmP")
    .standard(xLabels = labelsvA)
    .render()
    .write(new File
    ("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\Practicum\\Gráficas\\Datos Tipo Numérico/VoteCount.png"))

  BarChart
    .custom(years.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
    .title("Años MmP")
    .standard(xLabels = labelsvA)
    .render()
    .write(new File
    ("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\Practicum\\Gráficas\\Datos Tipo Numérico/Years.png"))

}
