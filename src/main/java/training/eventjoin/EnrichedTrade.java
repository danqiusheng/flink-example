package training.eventjoin;

public class EnrichedTrade implements Comparable<EnrichedTrade> {

    public EnrichedTrade() {
    }

    public EnrichedTrade(Trade trade, String customerInfo) {
        this.trade = trade;
        this.customerInfo = customerInfo;
    }

    public Trade trade;
    public String customerInfo;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("EnrichedTrade(").append(trade.timestamp).append(") ");
        sb.append(customerInfo);
        return sb.toString();
    }

    public int compareTo(EnrichedTrade other) {
        return Long.compare(this.trade.timestamp, other.trade.timestamp);
    }
}
