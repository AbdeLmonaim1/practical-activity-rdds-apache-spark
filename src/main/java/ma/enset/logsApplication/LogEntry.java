package ma.enset.logsApplication;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogEntry implements Serializable {
    private String ip;
    private String dateTime;
    private String method;
    private String resource;
    private int httpCode;
    private int responseSize;
    // Pattern pour parser une ligne de log Apache
    private static final String LOG_PATTERN =
            "^(\\S+) \\S+ \\S+ \\[([^\\]]+)\\] \"(\\S+) (\\S+) \\S+\" (\\d{3}) (\\d+|-).*";
    private static final Pattern pattern = Pattern.compile(LOG_PATTERN);
    public LogEntry(String ip, String dateTime, String method,
                    String resource, int httpCode, int responseSize) {
        this.ip = ip;
        this.dateTime = dateTime;
        this.method = method;
        this.resource = resource;
        this.httpCode = httpCode;
        this.responseSize = responseSize;
    }
    // Méthode statique pour parser une ligne de log
    public static LogEntry parseLogLine(String logLine) {
        Matcher matcher = pattern.matcher(logLine);

        if (matcher.matches()) {
            String ip = matcher.group(1);
            String dateTime = matcher.group(2);
            String method = matcher.group(3);
            String resource = matcher.group(4);
            int httpCode = Integer.parseInt(matcher.group(5));

            // Gérer le cas où la taille est "-"
            String sizeStr = matcher.group(6);
            int responseSize = sizeStr.equals("-") ? 0 : Integer.parseInt(sizeStr);

            return new LogEntry(ip, dateTime, method, resource, httpCode, responseSize);
        }

        return null; // Ligne invalide
    }
    public String getIp() {
        return ip;
    }

    public String getDateTime() {
        return dateTime;
    }

    public String getMethod() {
        return method;
    }

    public String getResource() {
        return resource;
    }

    public int getHttpCode() {
        return httpCode;
    }

    public int getResponseSize() {
        return responseSize;
    }

    public boolean isError() {
        return httpCode >= 400;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "ip='" + ip + '\'' +
                ", dateTime='" + dateTime + '\'' +
                ", method='" + method + '\'' +
                ", resource='" + resource + '\'' +
                ", httpCode=" + httpCode +
                ", responseSize=" + responseSize +
                '}';
    }

}
