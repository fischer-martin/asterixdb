package org.apache.asterix.runtime.evaluators.common;

public class JOFilterNode extends Node {

    private int height;
    // postorder ID of parent; -1 means no parent
    private int parent = -1;
    // postorder ID of favorable child
    private int favChild;
    // postorder ID of favorable child's left sibling
    private int favChildLeftSibling;
    // postorder ID of left sibling
    private int leftSibling = -1;

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int getParent() {
        return parent;
    }

    public void setParent(int parent) {
        this.parent = parent;
    }

    public int getFavChild() {
        return favChild;
    }

    public void setFavChild(int favChild) {
        this.favChild = favChild;
    }

    public int getFavChildLeftSibling() {
        return favChildLeftSibling;
    }

    public void setFavChildLeftSibling(int favChildLeftSibling) {
        this.favChildLeftSibling = favChildLeftSibling;
    }

    public int getLeftSibling() {
        return leftSibling;
    }

    public void setLeftSibling(int leftSibling) {
        this.leftSibling = leftSibling;
    }
}
