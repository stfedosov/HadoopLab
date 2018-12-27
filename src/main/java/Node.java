import java.util.ArrayList;
import java.util.List;

/**
 * @author sfedosov on 12/27/18.
 */
public class Node {
    private final int min;
    private final int max;
    private int geoname = -1;
    private final List<Node> children = new ArrayList<>();

    Node(int min, int max) {
        this.min = min;
        this.max = max;
    }

    public static void addChildren(Node prevNode, String[] split, int index, int netmask, int geoname) {
        if (index == split.length) {
            prevNode.setGeoname(geoname);
            return;
        }
        int min = Integer.parseInt(split[index]);
        for (Node node : prevNode.getChildren()) {
            if (node.getMin() == min) {
                addChildren(node, split, index + 1, netmask - 8, geoname);
                return;
            }
        }
        int max = min;
        if (netmask <= 0) {
            if (index == split.length - 1) {
                min = min == 0 ? 1 : min + 1;
            }
            max = (1 << 8) - (min == 0 ? 1 : 2);
        } else if (netmask < 8) {
            max = min + (1 << 8 - netmask) - 1;
        }
        Node toAdd = new Node(min, max);
        prevNode.addChild(toAdd);
        addChildren(toAdd, split, index + 1, netmask - 8, geoname);
    }

    int getGeoname() {
        return geoname;
    }

    int getMax() {
        return max;
    }

    void setGeoname(int geoname) {
        this.geoname = geoname;
    }

    int getMin() {
        return min;
    }

    void addChild(Node toAdd) {
        children.add(toAdd);
    }

    List<Node> getChildren() {
        return children;
    }
}