uniform.project("permafrost", "au.com.cba.omnia.permafrost")

uniformDependencySettings

libraryDependencies :=
  depend.hadoop() ++ depend.scalaz() ++ depend.testing() ++
  Seq(
    "org.apache.avro" % "avro-mapred" % "1.7.4"
  )

publishArtifact in Test := true

uniform.docSettings("https://github.com/CommBank/permafrost")

uniform.ghsettings
