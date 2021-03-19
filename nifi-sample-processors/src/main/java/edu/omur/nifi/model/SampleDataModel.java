package edu.omur.nifi.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;
import java.util.Map;

public class SampleDataModel {
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    private LocalDateTime timestamp;
    private String messageText;
    private Map<String, String> parameterList;

    public SampleDataModel() {
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getMessageText() {
        return messageText;
    }

    public void setMessageText(String messageText) {
        this.messageText = messageText;
    }

    public Map<String, String> getParameterList() {
        return parameterList;
    }

    public void setParameterList(Map<String, String> parameterList) {
        this.parameterList = parameterList;
    }

    @Override
    public String toString() {
        final String newLine = System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("timestamp: %s | messageText: %s", timestamp.toString(), messageText));
        if ((parameterList != null) && (parameterList.size() > 0)) {
            sb.append(String.format(" | parameters are: %s", newLine));
            parameterList.forEach((key, value) -> sb.append(String.format(" - key:%s, value:%s %s", key, value, newLine)));
        } else {
            sb.append(newLine);
        }
        return sb.toString();
    }
}
