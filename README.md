# WorldCongress 2019 Big Data Analytics with Spark Workshop

Repository for all my examples and instructions for a workshop on Spark at 
WorldCongress 2019. In order to run this, all you really need is a working version of Java (preferably Java 8) and
[sbt](https://www.scala-sbt.org/). If you want an IDE to code in, you can use
[VS Code](https://marketplace.visualstudio.com/items?itemName=scalameta.metals), [Eclipse](http://scala-ide.org/), or [IntelliJ](https://www.jetbrains.com/help/idea/discover-intellij-idea-for-scala.html).
These tools run on Windows, Mac, and Linux. If you are on a Window machine and
want to use Eclipse, I recommend using [MinGW](http://www.mingw.org/) as your command-line and not
the Windows/Ubuntu Bash extension because Bash uses a different path
configuration when you run "sbt compile eclipse" in the project to create the
Eclipse project. For my students I often recommend they install [SourceTree](https://www.sourcetreeapp.com/)
and use the terminal in there as that gives them MinGW and git working happily on a Windows machine.

You can also [download](http://spark.apache.org/downloads.html) and install Spark for command-line
access. I have examples in Python, and those benefit from having Spark installed on your machine.

The slides for the presentation are on [Google Docs](https://docs.google.com/presentation/d/1-8Rtnvjylec3UtV9Yb_YOmrJoGszT86W-54Szmr238Q/edit?usp=sharing).

There are four data sets used in the examples: [Adult Census Income Data](https://www.kaggle.com/uciml/adult-census-income), 
[BRFSS Health Survey](https://www.cdc.gov/brfss/), [Marvel Social Network](https://www.kaggle.com/csanhueza/the-marvel-universe-social-network),
and NOAA GHCN Weather Data(ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/) (in by_year I use 2018, 2017, 1968, 1918, 1868, and 1818).

Last year I put up some videos for my Big Data course you can find them on [YouTube](https://www.youtube.com/playlist?list=PLLMXbkbDbVt-f6qwCZqfq7e_6eT8aFxzT).
