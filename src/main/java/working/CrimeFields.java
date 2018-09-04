package working;

public enum CrimeFields {
    ID("ID"),
    CASE_NUMBER("Case Number"),
    DATE("Date"),
    BLOCK("Block"),
    IUCR("IUCR"),
    PRIMARY_TYPE("Primary Type"),
    DESCRIPTION("Description"),
    LOCATION_DESCRIPTION("Location Description"),
    ARREST("Arrest"),
    DOMESTIC("Domestic"),
    BEAT("Beat"),
    DISTRICT("District"),
    WARD("Ward"),
    COMMUNITY_AREA("Community Area"),
    FBI_CODE("FBI Code"),
    X_COORDINATE("X Coordinate"),
    Y_COORDINATE("Y Coordinate"),
    YEAR("Year"),
    UPDATED_ON("Updated On"),
    LATITUDE("Latitude"),
    LONGITUDE("Longitude"),
    LOCATION("Location");

    private String title;
    CrimeFields(String title) {
        this.title = title;
    }

    public String title() { return title; }

}
