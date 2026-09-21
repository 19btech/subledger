package com.reserv.dataloader.repository;

import com.reserv.dataloader.entity.UploadStatus;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface UploadStatusRepo extends MongoRepository<UploadStatus, String> {
    Optional<UploadStatus> findByUploadId(long uploadId);
}
