# Processin Data to the Server

![](https://img.shields.io/github/issues/pedroortizortega/Processin-data-to-the-server.svg) ![](https://img.shields.io/github/forks/pedroortizortega/Processin-data-to-the-server.svg) ![](https://img.shields.io/github/tag/pedroortizortega/Processin-data-to-the-server.svg) ![](https://img.shields.io/github/release/pedroortizortega/Processin-data-to-the-server.svg) ![](https://img.shields.io/github/stars/pedroortizortega/Processin-data-to-the-server.svg)

This is a file to process the data that is going to be in a database within the server. I used another file called PandasDataTool where the explanation of it is in the Python-Data-Toolkit repo.

## Description
###  FileProcessToCRM file
Within the file there are a some functions that make possible the procees fo the data. But, the principal function wich controls everything is called "processedFileToCRM". In this function you need to give some attributes to start the process as the name of the file, what characteristic you deside to process, etc. 

In this proyect, I had to use a simple NLP algorimth with RE and NLTK to process the "Address" attribute in the dataframe. I split the "Address" between the "street", "city", "state" and "zipcode". Then I made a template that my partnerts asked me with this new columns. At the end, I write the file into a csv file

## Used Libraries
- Numpy
- Pandas
- nltk
- re
- Glob