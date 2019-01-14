import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author sfedosov on 12/19/18.
 */
@Description(
        name = "getcountry"
)
public class GetCountryByIP extends UDF {

    private static final String GEONAME_TO_COUNTRY = "src/main/resources/geonametocountry.csv";
    private static final String IP_TO_GEONAME = "src/main/resources/iptogeoname.csv";
    private static final String DOT = "\\.";
    private static final String SLASH = "\\/";
    private static final List<Node> nodesList = new ArrayList<>();
    private static final Map<Integer, String> geonameToCountry = new HashMap<>();
    private Text EMPTY = new Text("empty");

    private static void fillMap(Map<Integer, String> map, String source) {
        try (Reader reader = Files.newBufferedReader(Paths.get(source));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT)) {
            for (CSVRecord csvRecord : csvParser) {
                if (!csvRecord.get(0).isEmpty()) {
                    map.put(Integer.valueOf(csvRecord.get(0)), csvRecord.get(5));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void readData() {
        try (Reader reader = Files.newBufferedReader(Paths.get(IP_TO_GEONAME));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withRecordSeparator("\t\n"))) {
            int prev = 0;
            Node prevNode = null;
            for (CSVRecord csvRecord : csvParser) {
                if (csvRecord.get(1).isEmpty() && csvRecord.get(2).isEmpty()) continue;
                String[] splitBySlash = csvRecord.get(0).split(SLASH);
                String[] split = splitBySlash[0].split(DOT);
                int key = Integer.valueOf(split[0]);
                int geoname = Integer.valueOf(csvRecord.get(1).isEmpty() ? csvRecord.get(2) : csvRecord.get(1));
                int netmask = Integer.valueOf(splitBySlash[1]);
                if (prev != key) {
                    prev = key;
                    if (prevNode != null) nodesList.add(prevNode.getChildren().get(0));
                    prevNode = null;
                }
                if (prevNode == null) prevNode = new Node(-1, -1); //fake node
                prevNode.addChildren(split, 0, netmask, geoname);
            }
            if (prevNode != null) {
                nodesList.add(prevNode.getChildren().get(0));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Text evaluate(Text ip) throws SQLException {
        if (nodesList.isEmpty()) {
            readData();
        }
        if (geonameToCountry.isEmpty()) {
            fillMap(geonameToCountry, GEONAME_TO_COUNTRY);
        }
        final String ipStr;
        if (ip == null || (ipStr = ip.toString().trim()).isEmpty()) return EMPTY;
        final String result = geonameToCountry.get(findGeoName(nodesList, null, ipStr.split(DOT), 0));
        return result == null ? EMPTY : new Text(result);
    }

    private static int findGeoName(List<Node> nodesList, Node prev, String[] ip, int index) {
        if (index == ip.length) return prev.geoname;
        int current = Integer.valueOf(ip[index]);
        int start = 0;
        int end = nodesList.size() - 1;
        while (start <= end) {
            int mid = start + (end - start) / 2;
            Node midNode = nodesList.get(mid);
            int min = midNode.min;
            int max = midNode.max;
            if (min <= current && max >= current) {
                return findGeoName(midNode.getChildren(), midNode, ip, index + 1);
            } else if (min > current) {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
        return -1;
    }

}
