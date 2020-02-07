package tp1;

public class Medicament {

    private int cip;
    private double prix;

    public Medicament (int cip, double prix){
        this.cip = cip;
        this.prix = prix;
    }

    public double getPrix() {
        return prix;
    }

    public int getCip() {
        return cip;
    }

}
