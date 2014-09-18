uniform.project("permafrost", "au.com.cba.omnia.permafrost")

uniformDependencySettings

libraryDependencies :=
  depend.scaldingproject() ++
  depend.scalaz() ++
  Seq(
    "au.com.cba.omnia" %% "omnitool-core" % "0.2.0-20140325115102",
    "au.com.cba.omnia" %% "tardis"        % "1.1.0-20140326134908",
    "au.com.cba.omnia" %% "omnia-test"    % "1.2.1-20140325113325" % "test"
  )

uniformAssemblySettings
