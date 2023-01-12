import java.time.LocalDate
import java.time.format.DateTimeFormatter
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.{DefaultFonts, DefaultTheme}
import com.github.tototoshi.csv._

import java.io.File


import com.cibo.evilplot.colors.RGB
import com.cibo.evilplot.geometry.{Align, Drawable, Extent, Rect, Text}
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.renderers.BarRenderer



object graficosDatos extends App {
  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()

  def average(valores: List[Double]): Double = {
    val t = valores.foldLeft((0.0, 0))((acc, currVal) => (acc._1 + currVal, acc._2 + 1))
    t._1 / t._2
  }

  //Presupuestos
  val presupuestos = data.flatMap(elem => elem.get("budget")).map(_.toDouble)
  val maximo = presupuestos.max
  val minimo = presupuestos.filter(_ > 0).min
  val promedio = average(presupuestos.filter(_ > 0))
  val presupuestoPc = Seq[Double](minimo, maximo, promedio)

  //VoteAverage
  val vote_average = data.flatMap(elem => elem.get("vote_average")).map(_.toDouble)
  val M= vote_average.max
  val m = vote_average.filter(_ > 0).min
  val p = average(vote_average.filter(_ > 0))
  val voteAverage = Seq[Double](m, M, p)
  val labelsvA = Seq("Minimo", "Maximo", "Promedio")

  //Original Lenguages
  val original_language = data.flatMap(elem => elem.get("original_language"))
  val frec = original_language.groupBy(x => x).map(t => (t._1, t._2.length)).toList.sortBy(_._2)(Ordering[Int].reverse)
  val languages: Seq[String] = frec.map(x => x._1)
  val values = frec.map(x=> x._2.toDouble).toSeq


  // Codigo para mostrar valores en las columnas
  val labeledByColor = new BarRenderer {
    val positive = RGB(241, 121, 6)
    val negative = RGB(226, 56, 140)
    def render(plot: Plot, extent: Extent, category: Bar): Drawable = {
      val rect = Rect(extent)
      val value = category.values.head
      val color = if (value >= 0) positive else negative
      Align.center(rect filled color, Text(s"$value", size = 10)
      ).group
    }
  }


  //Presupuestos
  BarChart
    .custom(presupuestoPc.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
    .title("Presupuestos MmP")
    .standard(xLabels = labelsvA)
    .render()
    .write(new File("C:\\Users\\Daniel\\Demos/Presupues.png"))


  //Vote Average
  BarChart
    .custom(voteAverage.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
    .title("Vote Average MmP")
    .standard(xLabels = labelsvA)
    .render()
    .write(new File("C:\\Users\\Daniel\\Demos/VoteAverage.png"))

  BarChart
    .custom(frec.map(x=> x._2.toDouble).toSeq.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
    .title("Originial Languajes")
    .standard(xLabels = frec.map(_._1).take(10))
    .xbounds(0, 10)
    .render()
    .write(new File("C:\\Users\\Daniel\\Demos/Lenguajes.png"))

}


