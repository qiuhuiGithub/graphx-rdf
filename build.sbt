version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.0.0"


// https://mvnrepository.com/artifact/org.apache.jena/jena-core
libraryDependencies += "org.apache.jena" % "jena-core" % "3.1.0"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
resolvers += "Repo at github.com/ankurdave/maven-repo" at "https://raw.githubusercontent.com/ankurdave/maven-repo/master"
libraryDependencies += "amplab" % "spark-indexedrdd" % "0.3"