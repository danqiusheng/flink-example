package cep.pojo;

public abstract class MonitoringEvent {

    private int rockID;

    private String machineName;

    public String getMachineName() {
        return machineName;
    }

    public void setMachineName(String machineName) {
        this.machineName = machineName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((machineName == null) ? 0 :
                machineName.hashCode());
        return result;
    }

    public int getRockID() {
        return rockID;
    }

    public void setRockID(int rockID) {
        this.rockID = rockID;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MonitoringEvent other = (MonitoringEvent) obj;
        if (machineName == null) {
            if (other.machineName != null)
                return false;
        } else if (!machineName.equals(other.machineName))
            return false;
        return true;
    }

    public MonitoringEvent(String machineName) {
        super();
        this.machineName = machineName;
    }


    public MonitoringEvent(int rockID, String machineName) {
        super();
        this.rockID = rockID;
        this.machineName = machineName;
    }
}