package com.microfocus.apollo.worker.prioritization.management;

import com.google.gson.annotations.SerializedName;

public class Component<T> {

    public Component(final String componentName, final String name, T value) {
        this.componentName = componentName;
        this.name = name;
        this.value = value;
    }
    
    @SerializedName("component")
    private String componentName;
    private String name;
    private T value;

    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}
