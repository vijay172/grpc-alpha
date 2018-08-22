package com.intel.flink.datatypes;

import org.apache.flink.api.java.tuple.Tuple3;

public class CameraTuple extends Tuple3<String, String, String> {

    public CameraTuple() {}

    public CameraTuple(final String cam, final String roi, final String camFileLocation) {
        this.f0 = cam;
        this.f1 = roi;
        this.f2 = camFileLocation;
    }

    public String getCamera() {
        return this.f0;
    }

    public void setCamera(String cam) {
        this.f0 = cam;
    }

    public String getRoi() {
        return this.f1;
    }

    public void setRoi(String roi) {
        this.f1 = roi;
    }

    public String getCamFileLocation() {
        return this.f2;
    }

    public void setCamFileLocation(String camFileLocation) {
        this.f2 = camFileLocation;
    }

    @Override
    public String toString() {
        return "CameraTuple{" +
                "f0=" + f0 +
                ", f1=" + f1 +
                ", f2=" + f2 +
                '}';
    }
}
