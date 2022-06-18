# LiRA Visualization Framework: Processing Pipeline
 
This repository contains the code for the Data Processing Pipeline as part of the Visualization framework for the LiRA Project. This is part of my bachelor thesis "Understanding Road Condition and Car Data: A Flexible Framework for the Visualization of Spatio-Temporal Data".


In the docs folder, the documentation for the pipeline can be found. It is accessible as a html file which can be found in docs/build/html.

In the pipeline folder, the source code for the pipeline can be found. It is divided in three packages:
- tables: Contains all modules for the table classes
- row_types: Contains all modules that implement subclasses for the different entity classes
- auxiliar_modules: Contains modules that provide auxiliar functionality such as database access or map_matching methods.

It also contains two files:
- main.py: Main script of the pipeline
- test.py: Basic test module which test the pipeline with local data (not available due to big size)
