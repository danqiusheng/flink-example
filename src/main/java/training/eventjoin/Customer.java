package training.eventjoin;

public class Customer implements Comparable<Customer> {

    public Customer() {
    }

    public Customer(Long timestamp, Long customerId, String customerInfo) {

        this.timestamp = timestamp;
        this.customerId = customerId;
        this.customerInfo = customerInfo;
    }

    public Long timestamp;
    public Long customerId;
    public String customerInfo;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Customer(").append(timestamp).append(") ");
        sb.append(customerInfo);
        return sb.toString();
    }

    public int compareTo(Customer other) {
        return Long.compare(this.timestamp, other.timestamp);
    }

}