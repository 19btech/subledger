package com.fyntrac.common.entity;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserRole {
    private String name;         // e.g., ADMIN, MANAGER, VIEWER
    private String description;
}
