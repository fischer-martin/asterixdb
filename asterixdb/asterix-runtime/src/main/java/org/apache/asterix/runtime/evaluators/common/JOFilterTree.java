package org.apache.asterix.runtime.evaluators.common;

import org.apache.commons.lang3.mutable.MutableInt;

import java.util.ArrayList;
import java.util.List;

public class JOFilterTree {

    private final List<JOFilterNode> postorderedTree;
    private int[] favChildOrderToPostorder;
    private final MutableInt arrIndex;

    public JOFilterTree() {
        postorderedTree = new ArrayList<>();
        arrIndex = new MutableInt(0);
    }

    public void reset() {
        postorderedTree.clear();
        arrIndex.setValue(0);
    }

    /**
     * This method MUST be called if changes have been made to the underlying List<JOFilterNode> and if the favorable
     * child order is required.
     */
    public void determineFavorableChildOrder() {
        if (favChildOrderToPostorder == null || postorderedTree.size() > favChildOrderToPostorder.length) {
            favChildOrderToPostorder = new int[postorderedTree.size()];
        }
        int rootPostID = postorderedTree.size() - 1;

        recurseInFavorableChildOrder(postorderedTree, rootPostID);
    }


    private void recurseInFavorableChildOrder(List<JOFilterNode> postorderedTree, int subtreeRootPostID) {
        JOFilterNode subtreeRoot = postorderedTree.get(subtreeRootPostID);

        if (subtreeRoot.getChildren().size() > 0) {
            // Initialize chPostIDLargestSubtree because the compiler does
            // not know that this will be read iff it has been initialized.
            int chPostIDLargestSubtree = -1;
            int chLargestSubtree = Integer.MIN_VALUE;
            for (int i = 0; i < subtreeRoot.getChildren().size(); ++i) {
                int currChildPostID = subtreeRoot.getChildren().getInt(i);
                if (postorderedTree.get(currChildPostID).getSubtreeSize() > chLargestSubtree) {
                    chPostIDLargestSubtree = currChildPostID;
                    chLargestSubtree = postorderedTree.get(currChildPostID).getSubtreeSize();
                }
            }

            recurseInFavorableChildOrder(postorderedTree, chPostIDLargestSubtree);

            for (int i = 0; i < subtreeRoot.getChildren().size(); ++i) {
                if (subtreeRoot.getChildren().getInt(i) == chPostIDLargestSubtree) {
                    continue;
                }
                recurseInFavorableChildOrder(postorderedTree, subtreeRoot.getChildren().getInt(i));
            }
        }

        favChildOrderToPostorder[arrIndex.intValue()] = subtreeRootPostID;
        arrIndex.increment();
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
