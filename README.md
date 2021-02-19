# Lambda Architecture for Sentiment Analysis

<p align="center">
  <img src="https://github.com/CosimoGiani/SentimentAnalysis_LambdaArchitecture/blob/master/img/lambda-architecture.png">
</p>

This project contains the implementation of a Lambda Architecture that allows to perform Twitter real-time sentiment analysis. To simulate the stream 
of tweets it was used the Twitter API, accessible through the *Twitter4J* library. \
In this scenario the architecture makes use of *Apache Hadoop* for the **Batch Layer**, *Apache Storm* for the **Speed Layer** and *Apache HBase* for the 
**Serving Layer**. The system exploits the *LingPipe* tool kit for processing text using computational linguistics to classify tweets. Then, to show the 
results it was implemented a graphical interface.

## Software requirements
* [Apache Hadoop 3.2.1](https://hadoop.apache.org/)
* [Apache Storm 2.1.0](https://storm.apache.org/)
* [Apache HBase 2.2.3](https://hbase.apache.org/)
* [LingPipe 4.1.0](http://www.alias-i.com/lingpipe/)
* [Twitter4J](http://twitter4j.org/en/)
* [JavaFX](https://openjfx.io/)

## Datasets
The following datasets were used during the development of this project:
* [Sentiment140](https://www.kaggle.com/kazanova/sentiment140)
* [Full-corpus.csv](https://github.com/guyz/twitter-sentiment-dataset)

## Usage
Get your personal Twitter Developer credentials and write them the ```TwitterCredentials.txt``` file. \
After having configured correctly and started Hadoop, Storm and HBase, execute in the following order:
* ```Classifier``` setting the datasets paths and the the file in which save the classifier
* ```Topology``` setting as args the keywords for the query 
* ```Driver``` no parameters need in args
* ```GUIinterface``` no parameters need in args

## Graphical User Interface
<p align="center">
  <img width="760" height="600" src="https://github.com/CosimoGiani/SentimentAnalysis_LambdaArchitecture/blob/master/img/gui.png">
</p>
