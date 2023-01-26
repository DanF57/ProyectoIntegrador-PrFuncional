import com.github.tototoshi.csv._
import java.io.File

import requests._
import scala.util.{Failure, Success, Try}
import play.api.libs.json._

import com.cibo.evilplot.plot._
import com.cibo.evilplot.colors.RGB
import com.cibo.evilplot.geometry.{Align, Drawable, Extent, Rect, Text}
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import com.cibo.evilplot.plot.{Bar, BarChart}
import com.cibo.evilplot.plot.renderers.BarRenderer
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.{DefaultFonts, DefaultTheme}


object DatosTexto extends App{
  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()


  //Meaningcloud para filtrar Cast - Actores
  def names(dataRaw: String): Option[String] = {
    val response: Response = requests
      .post("http://api.meaningcloud.com/topics-2.0",
        data = Map("key" -> "a103b77491e7d2674632370e369cf223",
          "lang" -> "en",
          "txt" -> dataRaw,
          "tt" -> "e"),
        headers = Map("content-type" -> "application/x-www-form-urlencoded"))
    Thread.sleep(500)
    if (response.statusCode == 200) {
      Option(response.text)
    } else
      Option.empty
  }

  //Columnas Tipo Texto
  val genres = data.flatMap(elem => elem.get("genres"))
  val original_language = data.flatMap(elem => elem.get("original_language"))
  val status = data.flatMap(elem => elem.get("status"))
  val title = data.flatMap(elem => elem.get("title"))
  val director = data.flatMap(elem => elem.get("director"))


  val cast = data
    .map((row) => row("cast"))
    .filter(_.nonEmpty)
    .map(StringContext.processEscapes)
    .take(100) //Restricción
    .map(names)
    .map(json => Try(Json.parse(json.get)))
    .filter(_.isSuccess)
    .map(_.get)
    .flatMap(json => json("entity_list").as[JsArray].value)
    .map(_("form"))
    .map(data => data.as[String])

  //Frecuencias
  val genFrec = frecuencia(genres)

  val langFrec = frecuencia(original_language)

  val statusFrec = frecuencia(status)

  val castFrec = frecuencia(cast)

  val direcFrec = frecuencia(director)


  //Consultar

  println(genFrec)
  println(langFrec)
  println(statusFrec)
  println(castFrec)
  println(direcFrec)



  //Funcion Obtener Frecuencia
  def frecuencia(columna: List[String]) : List[(String, Int)] = {
    columna
      .groupBy(x => x)
      .map(t => (t._1, t._2.length))
      .toList
      .filter(_._1.nonEmpty)
      .sortBy(_._2)
      .reverse
      .take(5) //Hacer Consulta más Corta (Top 10)
  }

  //Funcion Personalización Gráfico de Barras

  val labeledByColor = new BarRenderer {
    def render(plot: Plot, extent: Extent, category: Bar): Drawable = {
      val rect = Rect(extent)
      val value = category.values.head
      val color = RGB(0, 204, 204)
      Align.center(rect filled color, Text(s"$value", size = 10)
      ).group
    }
  }

  //Graficas
    //Variables para parámetros
    def barValues(data: List[(String, Int)]) = {
      data.map(_._2.toDouble).toSeq
    }
    val barGenres = barValues(genFrec)
    val barLanguages = barValues(langFrec)
    val barStatus = barValues(statusFrec)
    val barCast = barValues(castFrec)
    val barDirec = barValues(direcFrec)


  //Genres
  BarChart
    .custom(barGenres.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
    .title("Top 5 Géneros de Películas")
    .standard(xLabels = genFrec.map(_._1))
    .render()
    .write(
      new File("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\Practicum\\Gráficas\\Datos Tipo Texto/Top5Genres.png"))

  BarChart
    .custom(barLanguages.map(Bar.apply),
      spacing = Some(20),
      barRenderer = Some(labeledByColor))
    .title("Top 5 Idiomas de Películas")
    .standard(xLabels = langFrec.map(_._1))
    .render()
    .write(
      new File("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\Practicum\\Gráficas\\Datos Tipo Texto/Top5Languages.png"))

  BarChart
    .custom(barStatus.map(Bar.apply),
      spacing = Some(50),
      barRenderer = Some(labeledByColor))
    .title("Top 5 Status de Películas")
    .standard(xLabels = statusFrec.map(_._1))
    .render()
    .write(
      new File("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\Practicum\\Gráficas\\Datos Tipo Texto/Top5Status.png"))

  BarChart
    .custom(barCast.map(Bar.apply),
      spacing = Some(50),
      barRenderer = Some(labeledByColor))
    .title("Top 5 Actores de Películas")
    .standard(xLabels = castFrec.map(_._1))
    .render()
    .write(
      new File("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\Practicum\\Gráficas\\Datos Tipo Texto/Top5Actores.png"))

  BarChart
    .custom(barDirec.map(Bar.apply),
      spacing = Some(50),
      barRenderer = Some(labeledByColor))
    .title("Top 5 Directores de Películas")
    .standard(xLabels = direcFrec.map(_._1))
    .render()
    .write(
      new File("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\Practicum\\Gráficas\\Datos Tipo Texto/Top5Directores.png"))

}
