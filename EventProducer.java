import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static com.opencsv.CSVWriter.NO_QUOTE_CHARACTER;

/**
 * @author sfedosov on 12/27/18.
 */
public class EventProducer {

    private static final String[] categories = new String[]{
            "Accessories", "Clothes", "Shoes", "HandBags",
            "Watches", "Plus Sizes", "Home", "Bed & Bath", "Kids", "Juniors"};
    private static final Random random = new Random();
    private static int amountOfEvents = 500; //default

    public EventProducer() {
    }

    public static void main(String[] args) throws IOException, ParseException {
        String arg0 = args.length == 0 ? "" : args[0];
        if (arg0 != null && !arg0.isEmpty()) {
            amountOfEvents = Integer.parseInt(arg0);
        }
        createCSV(generateIpAddresses(amountOfEvents));
    }

    private static List<String> generateIpAddresses(int threshold) {
        List<String> result = new ArrayList<>();
        int i = 0;

        while (i++ < threshold) {
            int r1 = random.nextInt(224);
            int r4 = random.nextInt(256);
            result.add((r1 == 0 ? 1 : r1) + "." +
                    random.nextInt(256) + "." +
                    random.nextInt(256) + "." +
                    (r4 == 0 ? 1 : r4));
        }

        return result;
    }

    private static void createCSV(List<String> randomIp) {
        try (FileWriter fileWriter = new FileWriter(new File("CSV_DATA.csv"));
             CSVWriter csvWriter = new CSVWriter(fileWriter, ',', NO_QUOTE_CHARACTER)) {
            for (int j = 0; j < amountOfEvents; j++) {
                double ratio = random.nextGaussian();
                ratio = ratio < 0.0D ? ratio * -1.0D : ratio;
                int day = random.nextInt(8);
                String[] line = new String[]{"Product" + String.valueOf(random.nextInt(amountOfEvents / 10)),
                        (new DecimalFormat("##.##")).format(ratio * (double) amountOfEvents),
                        "" + new Timestamp((new Date("0" + (day == 0 ? 1 : day) + "/08/2018")).getTime()),
                        categories[random.nextInt(categories.length)], randomIp.get(j) + "\t"};
                csvWriter.writeNext(line);
                csvWriter.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
