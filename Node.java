import java.util.ArrayList;
import java.util.List;

/**
 * @author sfedosov on 12/27/18.
 */
public class Node {
    final int min;
    final int max;
    int geoname = -1;
    private final List<Node> children = new ArrayList<>();

    Node(int min, int max) {
        this.min = min;
        this.max = max;
    }

    public void addChildren(String[] split, int index, int netmask, int geoname) {
        if (index == split.length) {
            this.geoname = geoname;
            return;
        }
        int min = Integer.valueOf(split[index]);
        for (Node node : this.getChildren()) {
            if (node.min == min) {
                node.addChildren(split, index + 1, netmask - 8, geoname);
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
        Node newNode = new Node(min, max);
        this.addChild(newNode);
        newNode.addChildren(split, index + 1, netmask - 8, geoname);
    }

    void addChild(Node toAdd) {
        children.add(toAdd);
    }

    List<Node> getChildren() {
        return children;
    }
}
