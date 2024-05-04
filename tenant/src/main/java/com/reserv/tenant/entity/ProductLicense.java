package com.reserv.tenant.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "ProductLicense")
public class ProductLicense {
    @Id
    @Indexed(unique=true)
    private String id;
      private String licenseName;
  @Field(targetType = FieldType.DATE_TIME)
      private Date startDate;
  @Field(targetType = FieldType.DATE_TIME)
      private Date expiryDate;
      private Boolean tenantAllowed;
}
