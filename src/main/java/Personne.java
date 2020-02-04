import com.github.javafaker.Faker;

public class Personne {

    private String firstName;
    private String lastName;
    private String telephone;
    private String adresse;
    private int age;

    public Personne() {
        Faker faker = new Faker();
        this.firstName = faker.name().firstName();
        this.lastName = faker.name().lastName();
        this.telephone = faker.phoneNumber().cellPhone();
        this.adresse = faker.address().fullAddress();
        this.age=faker.number().numberBetween(15,70);

    }

    public Personne(String firstName, String lastName, String telephone, String adresse, int age) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.telephone = telephone;
        this.adresse = adresse;
        this.age = age;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getTelephone() {
        return telephone;
    }

    public String getAdresse() {
        return adresse;
    }

    public int getAge() {
        return age;
    }
}
