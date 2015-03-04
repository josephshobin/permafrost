uniform.project("permafrost", "au.com.cba.omnia.permafrost")

uniformDependencySettings

strictDependencySettings

libraryDependencies :=
  depend.hadoop() ++ depend.testing() ++
  depend.omnia("omnitool-core", "1.6.0-20150303125532-d9848dd") ++
  Seq(
    "org.apache.avro"   % "avro-mapred"   % "1.7.6",
    "au.com.cba.omnia" %% "omnitool-core" % "1.6.0-20150303125532-d9848dd" % "test" classifier "tests"
  )

updateOptions := updateOptions.value.withCachedResolution(true)

publishArtifact in Test := true

uniform.docSettings("https://github.com/CommBank/permafrost")

uniform.ghsettings
