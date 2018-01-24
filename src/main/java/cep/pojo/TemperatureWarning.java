package cep.pojo;


/**
 * 温度警告
 */
public class TemperatureWarning {

    private int rockID;

    private double temperature;

    public TemperatureWarning() {
    }

    public TemperatureWarning(int rockID, double temperature) {
        this.rockID = rockID;
        this.temperature = temperature;
    }

    public int getRockID() {
        return rockID;
    }

    public void setRockID(int rockID) {
        this.rockID = rockID;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }


    @Override
    public String toString() {
        return "TemperatureWarning{" +
                "rockID=" + rockID +
                ", temperature=" + temperature +
                '}';
    }
}
