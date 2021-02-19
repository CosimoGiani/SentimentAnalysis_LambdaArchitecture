package gui;

import javafx.animation.Animation;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.beans.property.SimpleStringProperty;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.util.Duration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.ResourceBundle;

public class GUI implements Initializable {
	
	// Batch
	@FXML
    private TableView<String[]> batchView;
    @FXML
    private TableColumn<String[], String> batchKeyword;
    @FXML
    private TableColumn<String[], String> batchPositive;
    @FXML
    private TableColumn<String[], String> batchNegative;

    // Real-time
    @FXML
    private TableView<String[]> realTimeView;
    @FXML
    private TableColumn<String[], String> realTimeKeyword;
    @FXML
    private TableColumn<String[], String> realTimePositive;
    @FXML
    private TableColumn<String[], String> realTimeNegative;

    // Chart
    @FXML
    private BarChart<String, Number> combinedViews;

    private Table realTimeTable;
    private Table batchTable;

    @Override
    public void initialize(URL url, ResourceBundle resource) {
        try {
            Configuration conf = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(conf);
            this.batchTable = connection.getTable(TableName.valueOf("batch_view"));
            this.realTimeTable = connection.getTable(TableName.valueOf("real_time_database"));

            // Batch View
            batchView.setPlaceholder(new Label("Empty Table"));
            batchKeyword.setCellValueFactory(p -> {
                String[] row = p.getValue();
                return new SimpleStringProperty(row[0]);
            });
            batchPositive.setCellValueFactory(p -> {
                String[] row = p.getValue();
                return new SimpleStringProperty(row[1]);
            });
            batchNegative.setCellValueFactory(p -> {
                String[] row = p.getValue();
                return new SimpleStringProperty(row[2]);
            });
            batchView.getItems().setAll(parseBatchResultsList(batchTable));

          // Real-time View
            realTimeView.setPlaceholder(new Label("Empty Table"));
            realTimeKeyword.setCellValueFactory(p -> {
                String[] row = p.getValue();
                return new SimpleStringProperty(row[0]);
            });
            realTimePositive.setCellValueFactory(p -> {
                String[] row = p.getValue();
                return new SimpleStringProperty(row[1]);
            });
            realTimeNegative.setCellValueFactory(p -> {
                String[] row = p.getValue();
                return new SimpleStringProperty(row[2]);
            });
            realTimeView.getItems().setAll(parseRealTimeResultsList(realTimeTable));

            // Chart
            XYChart.Series<String, Number> positiveSeries = new XYChart.Series<String, Number>();
            positiveSeries.setName("Positive");
            XYChart.Series<String, Number> negativeSeries = new XYChart.Series<String, Number>();
            negativeSeries.setName("Negative");
            for (String[] row: parseChartResults(batchTable, realTimeTable)) {
                positiveSeries.getData().add(new XYChart.Data<String, Number>(row[0], Integer.parseInt(row[1])));
                negativeSeries.getData().add(new XYChart.Data<String, Number>(row[0], Integer.parseInt(row[2])));
            }
            combinedViews.getData().addAll(positiveSeries, negativeSeries);

            // Refreshing to keep the GUI updated
            Timeline clock = new Timeline(new KeyFrame(Duration.ZERO, e -> {
                try {
                    realTimeView.getItems().setAll(parseRealTimeResultsList(realTimeTable));
                    batchView.getItems().setAll(parseBatchResultsList(batchTable));
                    realTimeView.refresh();
                    batchView.refresh();
                    positiveSeries.getData().clear();
                    negativeSeries.getData().clear();
                    combinedViews.setAnimated(false);
                    for (String[] row: parseChartResults(batchTable, realTimeTable)) {
                        positiveSeries.getData().add(new XYChart.Data<String, Number>(row[0], Integer.parseInt(row[1])));
                        negativeSeries.getData().add(new XYChart.Data<String, Number>(row[0], Integer.parseInt(row[2])));
                    }
                    combinedViews.getData().clear();
                    combinedViews.getData().addAll(positiveSeries, negativeSeries);
                } catch(IOException ex) {
                    ex.printStackTrace();
                }
            }),
                    new KeyFrame(Duration.seconds(1))
            );
            clock.setCycleCount(Animation.INDEFINITE);
            clock.play();

        } catch(IOException e) {
            e.printStackTrace();
        }

    }

    // Retrieving batch data
    private ArrayList<String[]> parseBatchResultsList(Table table) throws IOException {
        ArrayList<String[]> resultList = new ArrayList<>();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("sentiment_count"));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
            String rowKey = Bytes.toString(result.getRow());
            String positiveCount = Bytes.toString(result.getValue(Bytes.toBytes("sentiment_count"), Bytes.toBytes("positive")));
            String negativeCount = Bytes.toString(result.getValue(Bytes.toBytes("sentiment_count"), Bytes.toBytes("negative")));
            negativeCount = (negativeCount != null) ? negativeCount : "0";
            positiveCount = (positiveCount != null) ? positiveCount : "0";
            resultList.add(new String[]{rowKey, positiveCount, negativeCount});
        }
        resultList.sort(new Comparator<String[]>() {
            @Override
            public int compare(String[] firstString, String[] secondString) {
                return firstString[0].compareTo(secondString[0]);
            }
        });
        return resultList;
    }

    // Aggregating and retrieving real-time data
    private ArrayList<String[]> parseRealTimeResultsList(Table table) throws IOException {
        ArrayList<String[]> resultList = new ArrayList<>();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("content"));
        ResultScanner resultScanner = table.getScanner(scan);
        ArrayList<String> keywords = new ArrayList<>();
        ArrayList<Integer> positiveCount = new ArrayList<>();
        ArrayList<Integer> negativeCount = new ArrayList<>();
        for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
            String keyword = Bytes.toString(result.getValue(Bytes.toBytes("content"), Bytes.toBytes("keyword")));
            String sentimentString = Bytes.toString(result.getValue(Bytes.toBytes("content"), Bytes.toBytes("sentiment")));
            int sentiment = Integer.parseInt(sentimentString);
            if (keywords.contains(keyword)) {
                int index = keywords.indexOf(keyword);
                if (sentiment == 1) {
                    positiveCount.set(index, positiveCount.get(index) + 1);
                }
                else {
                    negativeCount.set(index, negativeCount.get(index) + 1);
                }
            }
            else {
                keywords.add(keyword);
                if (sentiment == 1) {
                    positiveCount.add(1);
                    negativeCount.add(0);
                }
                else {
                    positiveCount.add(0);
                    negativeCount.add(1);
                }
            }
        }
        for (int i = 0; i < keywords.size(); i++) {
            resultList.add(new String[]{keywords.get(i), positiveCount.get(i).toString(), negativeCount.get(i).toString()});
        }
        resultList.sort(new Comparator<String[]>() {
            @Override
            public int compare(String[] firstString, String[] secondString){
                return firstString[0].compareTo(secondString[0]);
            }
        });
        return resultList;
    }

    // Aggregating data chart
    private ArrayList<String[]> parseChartResults(Table batchTable, Table realTimeTable) throws IOException{
        ArrayList<String[]> chartResults = new ArrayList<>();
        ArrayList<String[]> batchResults = parseBatchResultsList(batchTable);
        ArrayList<String[]> realTimeResults = parseRealTimeResultsList(realTimeTable);
        for (String[] batchRow : batchResults) {
            int positiveCount = Integer.parseInt(batchRow[1]);
            int negativeCount = Integer.parseInt(batchRow[2]);
            for (String[] realTimeRow: realTimeResults) {
                if (batchRow[0].equals(realTimeRow[0])) {
                    positiveCount += Integer.parseInt(realTimeRow[1]);
                    negativeCount += Integer.parseInt(realTimeRow[2]);
                }
            }
            chartResults.add(new String[]{batchRow[0], String.valueOf(positiveCount), String.valueOf(negativeCount)});
        }
        for (String[] realTimeRow : realTimeResults) {
            boolean found = false;
            for (String[] chartRow : chartResults) {
                found = realTimeRow[0].equals(chartRow[0]);
                if (found) {
                    break;
                }
            }
            if (!found) {
                chartResults.add(realTimeRow);
            }
        }
        chartResults.sort(new Comparator<String[]>() {
            @Override
            public int compare(String[] firstString, String[] secondString) {
                return firstString[0].compareTo(secondString[0]);
            }
        });
        return chartResults;
    }
}
