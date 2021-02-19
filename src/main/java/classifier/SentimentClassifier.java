package classifier;

import com.aliasi.classify.Classification;
import com.opencsv.CSVReader;
import com.aliasi.classify.Classified;
import com.aliasi.classify.DynamicLMClassifier;
import com.aliasi.classify.LMClassifier;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.aliasi.lm.LanguageModel;
import com.aliasi.lm.NGramProcessLM;
import com.aliasi.stats.MultivariateDistribution;
import com.aliasi.util.AbstractExternalizable;
import com.aliasi.util.CommaSeparatedValues;


public class SentimentClassifier {

    public static final String TRAINING_FILE = "datasets/training.1600000.processed.noemoticon.csv";
    public static final String TEST_FILE = "datasets/full-corpus.csv";
    public static final String MODEL_FILE = "ClassifierModel.model";

    private static final String[] CATEGORIES = {"1", "0"};
    private static final int NGRAM_SIZE = 8;

    private DynamicLMClassifier<NGramProcessLM> trainingClassifier;
    private LMClassifier<LanguageModel, MultivariateDistribution> trainedClassifier;

    public SentimentClassifier() {
    	trainingClassifier = DynamicLMClassifier.createNGramProcess(CATEGORIES, NGRAM_SIZE);
    }

    public SentimentClassifier(File model) throws IOException, ClassNotFoundException {
        trainedClassifier = (LMClassifier<LanguageModel, MultivariateDistribution>) AbstractExternalizable.readObject(model);
    }

    // Training the classifier
    void train(String fileName) throws IOException {
        System.out.println("Training classifier");
        File file = new File(fileName);
        CommaSeparatedValues csv = new CommaSeparatedValues(file, "UTF-8");
        String[][] rows = csv.getArray();
        for(String[] row : rows){
            if(row.length != 6){
                continue;
            }
            String text = row[5];
            String sentiment = row[0];
            if (sentiment.equals("4")) {
                sentiment = "1";
            }
            Classification classification = new Classification(sentiment);
            Classified<CharSequence> classified = new Classified<CharSequence>(text, classification);
            trainingClassifier.handle(classified);
        }
    }

    // Evalueting sentiment and goodness of the trained model
    void evaluate(String fileName) throws IOException {
        System.out.println("Evaluating classifier");
        List<List<String>> records = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader(new FileReader(fileName))) {
            String[] values;
            while ((values = csvReader.readNext()) != null) {
                records.add(Arrays.asList(values));
            }
        }
        records.remove(0);
        int numTests = 0;
        int numCorrect = 0;
        for (List<String> row: records) {
            if (row.get(1).equals("irrelevant") || row.get(1).equals("neutral")) {
                continue;
            }
            numTests++;
            String text = row.get(4);
            String sentiment = row.get(1);
            if(sentiment.equals("positive")){
                sentiment = "1";
            }
            else{
                sentiment = "0";
            }
            Classification classification = trainedClassifier.classify(text);
            if (classification.bestCategory().equals(sentiment))
                ++numCorrect;
        }
        System.out.println("  # Test Cases=" + numTests);
        System.out.println("  # Correct=" + numCorrect);
        System.out.println("  % Correct=" + ((double)numCorrect)/(double)numTests);
    }

    // Method for saving the trained model
    public void save(String fileName) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(fileName);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        trainingClassifier.compileTo(objectOutputStream);
        objectOutputStream.close();
    }
    
    // Method used by the other classes for classifying a tweet
    public String classify(String tweet) {
        return trainedClassifier.classify(tweet).bestCategory();
    }

    public static void main(String[] args) throws Exception {
        File model = new File(MODEL_FILE);
        if (!model.exists()) {
    		SentimentClassifier classifier = new SentimentClassifier();
    		classifier.train(TRAINING_FILE);
    		classifier.save(MODEL_FILE);
    		System.out.println("Model stored");
    	} else {
    		SentimentClassifier classifier = new SentimentClassifier(model);
    		classifier.evaluate(TEST_FILE);
    	}
    }
}