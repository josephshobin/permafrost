uniform.project("permafrost", "au.com.cba.omnia.permafrost")

uniformDependencySettings

libraryDependencies :=
  depend.hadoop() ++ depend.testing() ++
  depend.omnia("omnitool-core", "1.6.0-20150211060329-d0909d8-CDH5") ++
  Seq(
    "org.apache.avro"   % "avro-mapred"   % "1.7.6",
    "au.com.cba.omnia" %% "omnitool-core" % "1.6.0-20150211060329-d0909d8-CDH5" % "test" classifier "tests"
  )

updateOptions := updateOptions.value.withCachedResolution(true)

publishArtifact in Test := true

uniform.docSettings("https://github.com/CommBank/permafrost")

uniform.ghsettings
