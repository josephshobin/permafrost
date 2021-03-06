uniform.project("permafrost", "au.com.cba.omnia.permafrost")

uniformDependencySettings

libraryDependencies :=
  depend.hadoop() ++ depend.scalaz() ++ depend.testing() ++
  depend.omnia("omnitool-core", "1.5.0-20150113041805-fef6da5") ++
  Seq(
    "org.apache.avro"   % "avro-mapred"   % "1.7.4",
    "au.com.cba.omnia" %% "omnitool-core" % "1.5.0-20150113041805-fef6da5" % "test" classifier "tests"
  )

updateOptions := updateOptions.value.withCachedResolution(true)

publishArtifact in Test := true

uniform.docSettings("https://github.com/CommBank/permafrost")

uniform.ghsettings
