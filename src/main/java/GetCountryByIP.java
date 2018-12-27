import com.opencsv.CSVReader;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStreamReader;
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

    private static final String GEONAME_TO_COUNTRY = "geonametocountry.csv";
    private static final String IP_TO_GEONAME = "iptogeoname.csv";
    private static final String DOT = "\\.";
    private static final String SLASH = "\\/";
    private static final List<Node> nodesList = new ArrayList<>();
    private static final char COMMA_SEPARATOR = ',';
    private static Map<Integer, String> geonameToCountry = new HashMap<>();
    private Text EMPTY = new Text("empty");

    private static void fillMap(Map<Integer, String> map, String source) {
        try (CSVReader reader =
                     new CSVReader(new InputStreamReader(GetCountryByIP.class.getResourceAsStream(source)), ',')) {
            String[] record;
            while ((record = reader.readNext()) != null) {
                if (!record[0].isEmpty()) {
                    map.put(Integer.valueOf(record[0]), record[5]);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void readData(List<Node> list) {
        try (CSVReader reader =
                     new CSVReader(new InputStreamReader(GetCountryByIP.class.getResourceAsStream(IP_TO_GEONAME)), COMMA_SEPARATOR)) {
            String[] record;
            int prev = 0;
            Node prevNode = null;
            while ((record = reader.readNext()) != null) {
                String[] splitBySlash = record[0].split(SLASH);
                String[] split = splitBySlash[0].split(DOT);
                int key = Integer.parseInt(split[0]);
                if (record[1].isEmpty() && record[2].isEmpty()) continue;
                int geoname = Integer.parseInt(record[1].isEmpty() ? record[2] : record[1]);
                int netmask = Integer.valueOf(splitBySlash[1]);
                if (prev != key) {
                    prev = key;
                    if (prevNode != null) list.add(prevNode.getChildren().get(0));
                    prevNode = null;
                }
                if (prevNode == null) prevNode = new Node(-1, -1); //fake node
                Node.addChildren(prevNode, split, 0, netmask, geoname);
            }
            if (prevNode != null) list.add(prevNode.getChildren().get(0));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Text evaluate(Text ip) throws SQLException {
        if (nodesList.isEmpty()) {
            readData(nodesList);
        }
        if (geonameToCountry.isEmpty()) {
            fillMap(geonameToCountry, GEONAME_TO_COUNTRY);
        }
        final String ipStr;
        if (ip == null || (ipStr = ip.toString().trim()).isEmpty()) return EMPTY;
        final String result = geonameToCountry.get(findGeoName(nodesList, null, ipStr.split(DOT), 0));
        return result == null ? EMPTY : new Text(result);
    }

    private static Integer findGeoName(List<Node> nodesList, Node prev, String[] ip, int index) {
        if (index == ip.length) return prev.getGeoname();
        Integer current = Integer.parseInt(ip[index]);
        int start = 0;
        int end = nodesList.size() - 1;
        while (start <= end) {
            int mid = start + (end - start) / 2;
            Node midNode = nodesList.get(mid);
            int min = midNode.getMin();
            int max = midNode.getMax();
            if (min <= current && max >= current) return findGeoName(midNode.getChildren(), midNode, ip, index + 1);
            else if (min > current) end = mid - 1;
            else start = mid + 1;
        }
        return -1;
    }

}
