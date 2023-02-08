
import com.github.tototoshi.csv._

import java.util.Locale
import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import com.cibo.evilplot.plot.renderers.BarRenderer
import com.cibo.evilplot.colors.RGB
import com.cibo.evilplot.geometry.{Align, Drawable, Extent, Rect, Text}
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.{DefaultFonts, DefaultTheme}

import java.util


object DatosNumericos extends App{

  //LECTURA CSV
  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()

  //FUNCIONES
  def average(valores: List[Long]): Double = {
    val t = valores.foldLeft((0.0, 0))((acc, currVal) => (acc._1 + currVal, acc._2 + 1))
    t._1 / t._2
  }

  def averageDouble(valores: List[Double]): Double = {
    val t = valores.foldLeft((0.0, 0))((acc, currVal) => (acc._1 + currVal, acc._2 + 1))
    t._1 / t._2
  }

  //Columnas Numéricas
  //Presupuestos
    //4803 valores
    //1037 ceros
  val presupuestos = data.flatMap(row => row.get("budget"))
    .map(_.toLong)
    //.count(_ == 0)

  //Popularity
    //4803 valores
    //1 ceros
  val popularity = data.flatMap(row => row.get("popularity"))
    .map(_.toDouble)
    //.count(_ == 0.0)

  //Release_date
    //4802 valores
    //1 vacios
  val release_date = data.flatMap(elem => elem.get("release_date"))
  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val releaseDateList = release_date
    .filter(!_.equals(""))
    .map(text => LocalDate.parse(text, dateFormatter))

  //Revenue
    //1427 ceros
  val revenue = data.flatMap(row => row.get("revenue"))
    .map(_.toLong)
    //.count(_ == 0)

  //Runtime
    //2 vacios
    //1427 ceros
  val runtime = data.flatMap(row => row.get("runtime"))
    .filter(!_.equals(""))
    .map(_.toDouble)

  //Vote_average
    //63 ceros
  val voteAverage = data.flatMap(row => row.get("vote_average"))
    .map(_.toDouble)
    //.count(_ == 0.0)

  //Vote_count
    //63 ceros
  val voteCount = data.flatMap(row => row.get("vote_count"))
    .map(_.toDouble)
    //.count(_ == 0.0)

  //------------------------PROMEDIOS-------------------------------
  val budgetAvg = average(presupuestos)
  val revenueAvg = average(revenue)

  //------------------------GRAFICAS--------------------------------
  //-----------REVENUE-----------
  val cerosRevenue = data.flatMap(row => row.get("revenue"))
    .map(_.toLong)
    .count(_ == 0)
    .toDouble

  val underAvgRevenue = revenue.count(_ < revenueAvg)
  val aboveAvgRevenue = revenue.count(_ > revenueAvg)

  PieChart(Seq("Ceros" -> cerosRevenue, "Bajo el Promedio" -> underAvgRevenue, "Encima del Promedio" -> aboveAvgRevenue))
    .rightLegend()
    .title("Revenue")
    .render()
    .write(new File
    ("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\New/Revenue.jpg"))

  //-----------BUDGET-----------
  val presupuestosAvg = average(presupuestos)

  val labeledByColor = new BarRenderer {
    val positive = RGB(0, 204, 204)
    val negative = RGB(0, 204, 204)

    def render(plot: Plot, extent: Extent, category: Bar): Drawable = {
      val rect = Rect(extent)
      val value = category.values.head
      val color = if (value >= presupuestosAvg) positive else negative
      Align.center(rect filled color, Text(s"$value%", size = 20)
      ).group
    }
  }

  val underAvgPres = "%.2f".format((presupuestos.count(_ < presupuestosAvg)/4803.0) * 100)
    .replace(",", ".").toDouble
  val aboveAvgPres = "%.2f".format((presupuestos.count(_ > presupuestosAvg)/4803.0) * 100)
    .replace(",", ".").toDouble
  val cerosPres = "%.2f".format((presupuestos.count(_ == 0)/4803.0) * 100)
    .replace(",", ".").toDouble


  val percentChange = Seq[Double](underAvgPres, aboveAvgPres, cerosPres)
  val labels = Seq("Menor", "Mayor", "Cero")
  BarChart
    .custom(percentChange.map(Bar.apply), spacing = Some(20),
      barRenderer = Some(labeledByColor)
    )
    .standard(xLabels = labels)
    .title("Porcentaje Presupuestos comparados al Promedio")
    .render()
    .write(new File
    ("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\New/Budgets.jpg"))

  //-----------RELEASE_DATE-----------
  val yearReleaseList = releaseDateList
    .map(_.getYear)
    .map(_.toDouble)

  Histogram(yearReleaseList)
    .title("Histograma Años de Lanzamiento")
    .xAxis()
    .yAxis()
    .xbounds(1916.0, 2018.0)
    .render()
    .write(new File("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\New/HistoYear.jpg"))

  //
}
