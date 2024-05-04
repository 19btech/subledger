package com.reserv.tenant.entity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "User")
public class User {

    @Id
    @Indexed(unique=true)
    private String id;
    @DBRef
    private Tenant tenant;

    private String firstName;
    private String lastName;
    private String email;
    private String pswd;
    private String roleId;
    private String authToken;
}
