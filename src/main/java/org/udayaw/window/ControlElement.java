package org.udayaw.window;

import java.io.Serializable;

public class ControlElement<T extends Object> implements Serializable {
    private boolean startNewWindow;
    private T element;

    public ControlElement(T element){
        this(element, false);
    }
    public ControlElement(T element, boolean startNewWindow) {
        this.startNewWindow = startNewWindow;
        this.element = element;
    }

    public boolean isStartNewWindow() {
        return startNewWindow;
    }

    public void setStartNewWindow(boolean startNewWindow) {
        this.startNewWindow = startNewWindow;
    }

    public T getElement() {
        return element;
    }

    public void setElement(T element) {
        this.element = element;
    }

}
