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
@Document(collection = "Tenant")
public class Tenant {
    @Id
    @Indexed(unique=true)
    private String id;
    @DBRef
    private Merchant merchant;

    private String tenantName;
    private String tenantDescription;
    private boolean isActivated;
    private Date creationDate;
    private Date endDate;

}
