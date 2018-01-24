package cep.pojo;

/**
 * 报警
 */
public class TemperatureAlert {
    private int rockID;

    public int getRockID() {
        return rockID;
    }

    public void setRockID(int rockID) {
        this.rockID = rockID;
    }

    public TemperatureAlert(int rockID) {
        this.rockID = rockID;
    }

    public TemperatureAlert(){

    }

    @Override
    public String toString() {
        return "TemperatureAlert{" +
                "rockID=" + rockID +
                '}';
    }
}
