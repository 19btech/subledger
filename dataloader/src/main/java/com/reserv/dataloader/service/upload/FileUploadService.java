package com.reserv.dataloader.service.upload;

import  com.fyntrac.common.config.TenantContextHolder;
import  com.fyntrac.common.enums.AccountingRules;
import com.reserv.dataloader.utils.ExcelFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class FileUploadService {

    @Value("${spring.batch.upload.files.directory}")
    private String batchFilesDirectory;
    @Autowired
    private final TenantContextHolder tenantContextHolder;
    UploadService transactionsUploadService;
    ActivityUploadService activityUploadService;
    FileUploadService(TenantContextHolder tenantContextHolder
            , TransactionsUploadService transactionsUploadService
            , ActivityUploadService activityUploadService) {
        this.tenantContextHolder = tenantContextHolder;
        this.transactionsUploadService = transactionsUploadService;
        this.activityUploadService = activityUploadService;
    }
    public void uploadFiles(MultipartFile ... files) throws Throwable {

        String FOLDER_PATH = System.getProperty("user.home") + File.separator + "tenants" + File.separator + tenantContextHolder.getTenant() + File.separator;
        String OUTPUT_FOLDER_PATH = System.getProperty("user.home") + File.separator + "output" + File.separator + "tenants" + File.separator + tenantContextHolder.getTenant() + File.separator;

        Set<String> validFileSet = new HashSet<>(0);
        Set<String> inValidFileSet = new HashSet<>(0);
        boolean isValidFile = Boolean.FALSE;

        for(MultipartFile file : files) {
            if(ExcelFileUtil.isZipFile(file)) {
                Set<File> dataFiles =  ExcelFileUtil.unzip(file, FOLDER_PATH);
                for (File f : dataFiles) {
                        validFileSet.add(f.getAbsolutePath());
                }
            }else {
                validFileSet.add(ExcelFileUtil.convertMultipartFileToFile(file,FOLDER_PATH));
            }
        }

        this.convertIntoCSVFiles(validFileSet);
        List<Path> fileList = ExcelFileUtil.listCsvFiles(OUTPUT_FOLDER_PATH, ".csv");
        // Create a Map<AccountingRules, filePath>
        Map<AccountingRules, String> rulesMap = new HashMap<>();

        for(Path file : fileList) {
            boolean isValidRule = AccountingRules.isValid(file.getFileName().toString().toLowerCase());
            if(isValidRule) {
                AccountingRules rule = AccountingRules.get(file.getFileName().toString().toLowerCase());
                assert rule != null;
                rulesMap.put(rule, file.toString());
            }
        }

        // Sort the map by priority (higher number = higher priority)
        Map<AccountingRules, String> sortedMap = rulesMap.entrySet()
                .stream()
                .sorted(Map.Entry.<AccountingRules, String>comparingByKey(Comparator.comparingInt(AccountingRules::getPriority).reversed()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1, // Merge function (not used here)
                        LinkedHashMap::new // Maintain insertion order
                ));

        Map<AccountingRules, String> activityMap = new HashMap<>(0);
        for(Map.Entry<AccountingRules,String> entry : sortedMap.entrySet()) {
            AccountingRules rule = entry.getKey();
            boolean isActivity = (rule == AccountingRules.TRANSACTIONACTIVITY || rule == AccountingRules.INSTRUMENTATTRIBUTE);
            String file = entry.getValue();
            if(isActivity) {
                activityMap.put(rule, file);
            }else{
                UploadService uploadService = UploadServiceFactory.getFileUploader(rule);
                uploadService.uploadData(file);
            }
        }

        if(activityMap != null && !activityMap.isEmpty()) {
            this.activityUploadService.uploadActivity(activityMap);
        }


    }

    private void convertIntoCSVFiles(Set<String> fileList) throws Throwable {
        String OUTPUT_FOLDER_PATH = System.getProperty("user.home") + File.separator + "output" + File.separator + "tenants" + File.separator + tenantContextHolder.getTenant() + File.separator;

        for(String file :  fileList) {
            if(ExcelFileUtil.isExtensionMatched(file,"csv")) {
                ExcelFileUtil.moveFileToFolder(file,OUTPUT_FOLDER_PATH);
            }else if(ExcelFileUtil.isExtensionMatched(file,"xls") ||
                    ExcelFileUtil.isExtensionMatched(file,"xlsx")){
                FileUtils.deleteDirectory(new File(OUTPUT_FOLDER_PATH));
                ExcelFileUtil.convertExcelToCSV(file,OUTPUT_FOLDER_PATH, 1L);
            }
        }
    }
}
