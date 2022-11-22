package org.apache.asterix.runtime.evaluators.common;

import java.util.List;

public class JOFilterTree {

    private final List<JOFilterNode> postorderedTree;
    private final int[] favChildOrderToPostorder;

    public JOFilterTree(List<JOFilterNode> postorderedTree, int[] favChildOrderToPostorder) {
        this.postorderedTree = postorderedTree;
        this.favChildOrderToPostorder = favChildOrderToPostorder;
    }

    public List<JOFilterNode> getPostorderedTree() {
        return postorderedTree;
    }

    /**
     * Converts a given postorder ID to the corresponding favorable child order ID.
     *
     * @param postorderID postorder ID of the node that the favorable child order ID should be retrieved of.
     * @return favorable child order ID for postorderID
     */
    public int favChildOrderToPostorder(int postorderID) {
        return favChildOrderToPostorder[postorderID];
    }

}
