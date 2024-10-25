package com.reserv.dataloader.service.upload;

import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.enums.AccountingRules;
import com.reserv.dataloader.utils.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@Service
public class FileUploadService {

    @Value("${spring.batch.upload.files.directory}")
    private String batchFilesDirectory;
    @Autowired
    private final TenantContextHolder tenantContextHolder;
    UploadService transactionsUploadService;
    FileUploadService(TenantContextHolder tenantContextHolder, TransactionsUploadService transactionsUploadService) {
        this.tenantContextHolder = tenantContextHolder;
        this.transactionsUploadService = transactionsUploadService;
    }
    public void uploadFiles(MultipartFile ... files) throws Throwable {

        String FOLDER_PATH = System.getProperty("user.home") + File.separator + "tenants" + File.separator + tenantContextHolder.getTenant() + File.separator;
        String OUTPUT_FOLDER_PATH = System.getProperty("user.home") + File.separator + "output" + File.separator + "tenants" + File.separator + tenantContextHolder.getTenant() + File.separator;

        Set<String> validFileSet = new HashSet<>(0);
        Set<String> inValidFileSet = new HashSet<>(0);
        boolean isValidFile = Boolean.FALSE;

        for(MultipartFile file : files) {
            if(FileUtil.isZipFile(file)) {
                Set<File> dataFiles =  FileUtil.unzip(file, FOLDER_PATH);
                for (File f : dataFiles) {
                        validFileSet.add(f.getAbsolutePath());
                }
            }else {
                validFileSet.add(FileUtil.convertMultipartFileToFile(file,FOLDER_PATH));
            }
        }

        this.convertIntoCSVFiles(validFileSet);
        List<Path> fileList = FileUtil.listCsvFiles(OUTPUT_FOLDER_PATH, ".csv");
        for(Path file : fileList) {
            boolean isValidRule = AccountingRules.isValid(file.getFileName().toString().toLowerCase());
            if(isValidRule) {
                AccountingRules rule = AccountingRules.get(file.getFileName().toString().toLowerCase());
                assert rule != null;
                UploadService uploadService =  UploadServiceFactory.getFileUploader(rule);
                uploadService.uploadData(file.toString());
            }
        }
    }

    private void convertIntoCSVFiles(Set<String> fileList) throws Throwable {
        String OUTPUT_FOLDER_PATH = System.getProperty("user.home") + File.separator + "output" + File.separator + "tenants" + File.separator + tenantContextHolder.getTenant() + File.separator;

        for(String file :  fileList) {
            if(FileUtil.isExtensionMatched(file,"csv")) {
                FileUtil.moveFileToFolder(file,OUTPUT_FOLDER_PATH);
            }else if(FileUtil.isExtensionMatched(file,"xls") ||
                    FileUtil.isExtensionMatched(file,"xlsx")){
                FileUtils.deleteDirectory(new File(OUTPUT_FOLDER_PATH));
                FileUtil.convertExcelToCSV(file,OUTPUT_FOLDER_PATH, 1L);
            }
        }
    }
}
