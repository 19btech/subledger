package com.reserv.tenant.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "Merchant")
public class Merchant {

    @Id
    @Indexed(unique=true)
    private String id;
    private String merchantName;
    private String webDomain;
    // @DBRef
    // private ProductLicense productLicense;
    private String address;
    private String phoneNumbers;
    private String contactPerson;
    private String email;
}
