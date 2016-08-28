package com.evolveum.polygon.connector.drupal;

/**
 * Created by gpalos on 18. 8. 2016.
 */
public class DrupalFilter {
    public String byName;
    public String byUid;
    public String byEmailAddress;

    @Override
    public String toString() {
        return "DrupalFilter{" +
                "byName='" + byName + '\'' +
                ", byUid=" + byUid +
                ", byEmailAddress='" + byEmailAddress + '\'' +
                '}';
    }
}
