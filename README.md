# ProyectoIntegrador-PrFuncional
Integrantes: Daniel Flores, Cristian Rodríguez, Jean Panamito

Dependencias necesarias para el build 
```scala
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "Proyecto_Integrador",
      libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.10",
      libraryDependencies += "io.github.cibotech" %% "evilplot" % "0.8.1",
      libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.3",
      libraryDependencies += "com.lihaoyi" %% "requests" % "0.8.0",
        libraryDependencies += "org.scalikejdbc" %% "scalikejdbc" % "4.0.0",
        libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3",
        libraryDependencies += "com.mysql" % "mysql-connector-j" % "8.0.31"
)
```
¡Claro! Puedo ayudarte a crear una tabla en formato Markdown para la descripción de los datos en tu wiki de GitHub. Aquí tienes un ejemplo de cómo podrías estructurar la tabla con la información que tenemos:

| Campo                | Tipo de Dato | Etiquetas                  | Propósito                                       | Observación                                          |
| -------------------- | ------------ | -------------------------- | ----------------------------------------------- | ---------------------------------------------------- |
| id_vivienda          | Entero       | Identificador              | Identificar de forma única una vivienda          |                                                      |
| estrato              | Entero       | Estrato Socioeconómico     | Indicar el estrato socioeconómico de la vivienda |                                                      |
| id_hogar             | Entero       | Identificador              | Identificar de forma única un hogar              |                                                      |
| vivienda_id          | Entero       | Clave foránea              | Vincular con la entidad Vivienda                 |                                                      |
| periodo              | Entero       | Período                    | Indicar el período de la encuesta                |                                                      |
| mes                  | Texto        | Mes                        | Indicar el mes de la encuesta                    |                                                      |
| obtiene_agua         | Texto        | Fuente de Obtención de Agua | Indicar la fuente de obtención de agua           |                                                      |
| tiene_medidor_agua   | Booleano     |                            | Indicar si la vivienda tiene medidor de agua     |                                                      |
| agua_junta           | Booleano     |                            | Indicar si el agua proviene de la junta de agua  |                                                      |
| servicio_ducha       | Texto        | Tipo de Servicio de Ducha  | Indicar el tipo de servicio de ducha             |                                                      |
| tipo_alumbrado       | Texto        | Tipo de Alumbrado          | Indicar el tipo de alumbrado de la vivienda      |                                                      |
| elimina_basura       | Texto        | Forma de Eliminación       | Indicar la forma de eliminación de basura        |                                                      |
| forma_tenencia       | Texto        | Forma de Tenencia          | Indicar la forma de tenencia de la vivienda      |                                                      |
| valor_arriendo       | Decimal      |                            | Indicar el valor mensual de arriendo             |                                                      |
| incluye_agua_arriendo| Booleano     |                            | Indicar si el arriendo incluye pago de agua      |                                                      |
| incluye_luz_arriendo | Booleano     |                            | Indicar si el arriendo incluye pago de luz       |                                                      |
| parentesco_propietario| Texto       | Parentesco Propietario      | Indicar la relación de parentesco con el propietario |                                                  |
| tiene_vehiculos      | Booleano     |                            | Indicar si el hogar tiene vehículos              |                                                      |
| cantidad_vehiculos   | Entero       |                            | Indicar la cantidad de vehículos del hogar       |                                                      |
| tiene_motos          | Booleano     |                            | Indicar si el hogar tiene motos                  |                                                      |
| cantidad_motos       | Entero       |                            | Indicar la cantidad de motos del hogar           |                                                      |
| tipo_combustible     | Texto        | Tipo de Combustible        | Indicar el tipo de abastecimiento utilizado      |                                                      |
| gasto_combustible    | Decimal      | Gasto de Combustible       | Indicar el gasto mensual en combustible          |                                                      |

Ten en cuenta que esta tabla es solo un ejemplo y deberás adaptarla a tu proyecto según tus propias entidades y atributos. Además, puedes agregar o eliminar columnas según tus necesidades.
