import java.util.List;

public class Family {
    public String lastname;
    public List<Person> members;


    public Family(String lastname, List<Person> members) {
        this.lastname = lastname;
        this.members = members;
    }

    public List<Person> getMembers() {
        return members;
    }


}

