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
    private static final List<long[]> nodesList = new ArrayList<>(305376);
    private static Map<Integer, String> geonameToCountry = new HashMap<>(252);
    private Text EMPTY = new Text("empty");

    private static void fillMap(Map<Integer, String> map, String source) {
        try (CSVReader reader =
                     new CSVReader(
                             new InputStreamReader(
                                     GetCountryByIP.class.getResourceAsStream(source)))) {
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
        
    private static void readData() {
        try (CSVReader reader =
                     new CSVReader(
                             new InputStreamReader(
                                     GetCountryByIP.class.getResourceAsStream(IP_TO_GEONAME)))) {
            String[] record;
            while ((record = reader.readNext()) != null) {
                if (record[1].isEmpty() && record[2].isEmpty()) continue;
                String[] splitBySlash = record[0].split(SLASH);
                long min = ipToLong(splitBySlash[0].split(DOT));
                long max = min + (1 << 32 - Integer.valueOf(splitBySlash[1]));
                int geoname = Integer.valueOf(record[1].isEmpty() ? record[2] : record[1]);
                nodesList.add(new long[]{min, max, geoname});
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static long ipToLong(String[] ipStr) {
        return Long.valueOf(ipStr[3])
                | Long.valueOf(ipStr[2]) << 8
                | Long.valueOf(ipStr[1]) << 16
                | Long.valueOf(ipStr[0]) << 24;
    }

    public Text evaluate(Text ip) throws SQLException {
        if (nodesList.isEmpty()) readData();
        if (geonameToCountry.isEmpty()) fillMap(geonameToCountry, GEONAME_TO_COUNTRY);
        final String ipStr;
        if (ip == null || (ipStr = ip.toString().trim()).isEmpty()) return EMPTY;
        final String result = geonameToCountry.get(findGeoName(ipToLong(ipStr.split(DOT))));
        return result == null ? EMPTY : new Text(result);
    }

    private static int findGeoName(long toSearch) {
        int start = 0;
        int end = nodesList.size() - 1;
        while (start <= end) {
            int mid = start + (end - start) / 2;
            long[] midNode = nodesList.get(mid);
            if (midNode[0] <= toSearch && midNode[1] >= toSearch) {
                return (int) midNode[2];
            } else if (midNode[0] > toSearch) {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
        return -1;
    }

}
