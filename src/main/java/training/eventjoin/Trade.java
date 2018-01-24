package training.eventjoin;

public class Trade {

    public Trade() {}

    public Trade(Long timestamp, Long customerId, String tradeInfo) {

        this.timestamp = timestamp;
        this.customerId = customerId;
        this.tradeInfo = tradeInfo;
    }

    public Long timestamp;
    public Long customerId;
    public String tradeInfo;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Trade(").append(timestamp).append(") ");
        sb.append(tradeInfo);
        return sb.toString();
    }
}