@echo off
REM * Patric Conrad
REM * Ekagra Software Technologies
REM * April 17, 2012
REM *
REM * This file is used to create the Java Archive (JAR) file for the BTRIS Data Extractor for C3D.
REM * It includes are necessary files for deploying/installing the application.
REM *

> jar_builder.log  echo * * *
>> jar_builder.log echo * BTRIS Date Extractor for C3D Jar Builder
>> jar_builder.log echo * * *
>> jar_builder.log echo Starting: %Date% -- %Time%

>> jar_builder.log echo * * *
>> jar_builder.log echo jar cvf ../Release/BTRISExtractor.jar *.class Run_BTRIS*.bat
>> jar_builder.log jar cvf ../Release/BTRISExtractor.jar *.class Run_BTRIS*.bat

>> jar_builder.log echo * * *
>> jar_builder.log echo jar uvfm ../Release/BTRISExtractor.jar Manifest.txt
>> jar_builder.log jar uvfm ../Release/BTRISExtractor.jar Manifest.txt

>> jar_builder.log echo * * *
>> jar_builder.log echo jar uvf ../Release/BTRISExtractor.jar ojdbc14.jar sqljdbc4.jar BDE.properties 
>> jar_builder.log jar uvf ../Release/BTRISExtractor.jar ojdbc14.jar sqljdbc4.jar BDE.properties 

>> jar_builder.log echo * * *
>> jar_builder.log echo jar uvf ../Release/BTRISExtractor.jar ojdbc14.jar sqljdbc4.jar BDE.properties 
>> jar_builder.log jar uvf ../Release/BTRISExtractor.jar ojdbc14.jar sqljdbc4.jar BDE.properties 

>> jar_builder.log echo * * *
>> jar_builder.log echo jar uvf ../Release/BTRISExtractor.jar v1.0.4tov1.1BTRIS.zip
>> jar_builder.log jar uv0f ../Release/BTRISExtractor.jar v1.0.4tov1.1BTRIS.zip

>> jar_builder.log echo * * *
>> jar_builder.log echo Finished: %Date% -- %Time%

more jar_builder.log
