package c23_1.files.utils;

public class Utils {
    public static String getUTCDateAsString(String date) {
        String[] accountCreatedDate = date.split(" ");
        String[] dateArray = accountCreatedDate[0].split("/");
        String year = dateArray[2].length() == 2 ? "20" + dateArray[2] : dateArray[2];
        String month = dateArray[0].length() == 2 ? dateArray[0] : "0" + dateArray[0];
        String day = dateArray[1].length() == 2 ? dateArray[1] : "0" + dateArray[1];
        String[] timeArray = accountCreatedDate[1].split(":");
        String hours = timeArray[0].length() == 2 ? timeArray[0] : "0" + timeArray[0];
        String minutes = timeArray[1];

        return year + "-" + month + "-" + day + "T" + hours + ":" + minutes + ":00";
    }
}
