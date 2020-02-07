package tp1;

public class Pharmacie {

    private int id;
    private String nom;
    private String adresse;
    private String depart;
    private String region;

    public Pharmacie(int id, String nom, String adresse, String depart, String region){
        this.id = id;
        this.nom = nom;
        this.adresse = adresse;
        this.depart = depart;
        this.region = region;
    }



    public int getId() {
        return id;
    }

    public String getAdresse() {
        return adresse;
    }

    public String getDepart() {
        return depart;
    }

    public String getNom() {
        return nom;
    }

    public String getRegion() {
        return region;
    }
}
