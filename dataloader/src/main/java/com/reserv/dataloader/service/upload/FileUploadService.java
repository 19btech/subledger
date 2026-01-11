package com.reserv.dataloader.service.upload;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.enums.AccountingRules;
import com.fyntrac.common.repository.CustomTableDefinitionRepository;
import com.fyntrac.common.utils.FileUtil;
import com.reserv.dataloader.exception.CustomTableNotFoundException;
import com.reserv.dataloader.utils.ExcelFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

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
    protected final TenantContextHolder tenantContextHolder;
    private final ActivityUploadService activityUploadService;
    private final CustomTableDefinitionRepository customTableDefinitionRepository;
    private final TransactionsUploadService transactionsUploadService;
    FileUploadService(TenantContextHolder tenantContextHolder
            , ActivityUploadService activityUploadService,
                      TransactionsUploadService transactionsUploadService,
                      CustomTableDefinitionRepository customTableDefinitionRepository) {
        this.tenantContextHolder = tenantContextHolder;
        this.activityUploadService = activityUploadService;
        this.customTableDefinitionRepository = customTableDefinitionRepository;
        this.transactionsUploadService = transactionsUploadService;
    }
    public void uploadFiles(MultipartFile ... files) throws Throwable {

        String FOLDER_PATH = System.getProperty("user.home") + File.separator + "tenants" + File.separator + tenantContextHolder.getTenant() + File.separator;
        String OUTPUT_FOLDER_PATH = System.getProperty("user.home") + File.separator + "output" + File.separator + "tenants" + File.separator + tenantContextHolder.getTenant() + File.separator;
        long uploadId = Long.parseLong(
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"))
        );

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

        this.convertIntoCSVFiles(validFileSet, Boolean.TRUE);
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
                uploadService.uploadData(uploadId, file);
            }
        }

        if(activityMap != null && !activityMap.isEmpty()) {
            this.activityUploadService.uploadActivity(uploadId, activityMap);
        }


    }


    public void uploadCustomTableDataFiles(MultipartFile ... files) throws Throwable {

        String FOLDER_PATH = System.getProperty("user.home") + File.separator + "tenants" + File.separator + tenantContextHolder.getTenant() + File.separator;
        String OUTPUT_FOLDER_PATH = System.getProperty("user.home") + File.separator + "output" + File.separator + "tenants" + File.separator + tenantContextHolder.getTenant() + File.separator;

        FileUtil.createDirectoryIfNotExists(FOLDER_PATH);
        FileUtil.createDirectoryIfNotExists(OUTPUT_FOLDER_PATH);
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

        this.convertIntoCSVFiles(validFileSet, Boolean.FALSE);
        List<Path> fileList = ExcelFileUtil.listCsvFiles(OUTPUT_FOLDER_PATH, ".csv");
        // Create a Map<AccountingRules, filePath>

        List<CustomTableDefinition> customTables = this.customTableDefinitionRepository.findAll();

        Map<String, CustomTableDefinition> tableMap =
                customTables.stream()
                        .filter(Objects::nonNull)
                        .filter(t -> t.getTableName() != null)
                        .collect(Collectors.toMap(
                                t -> t.getTableName().toLowerCase(Locale.ROOT), // ✅ case-insensitive key
                                Function.identity(),
                                (existing, replacement) -> existing,            // ✅ handle duplicates safely
                                LinkedHashMap::new                               // ✅ keeps order
                        ));

        Map<CustomTableDefinition, String> customTableMap = new HashMap<>();
        for(Path file : fileList) {
            String fileName = FileUtil.getFileNameWithoutExtension(file);
            if(fileName != null) {
                if (tableMap.containsKey(fileName.toLowerCase())) {
                    CustomTableDefinition customTableDefinition = tableMap.get(fileName.toLowerCase());
                    customTableMap.put(customTableDefinition, file.toString());
                }else{
                    inValidFileSet.add(fileName);
                }
            }
        }


        if (!inValidFileSet.isEmpty()) {
            throw new CustomTableNotFoundException(
                    "Custom table not found for: " + String.join(", ", inValidFileSet)
            );
        } else if(!customTableMap.isEmpty()) {
            this.activityUploadService.uploadCustomTableData(customTableMap);
        }


    }

    private void convertIntoCSVFiles(Set<String> fileList, boolean validate) throws Throwable {
        String OUTPUT_FOLDER_PATH = System.getProperty("user.home") + File.separator + "output" + File.separator + "tenants" + File.separator + tenantContextHolder.getTenant() + File.separator;

        for(String file :  fileList) {
            if(ExcelFileUtil.isExtensionMatched(file,"csv")) {
                ExcelFileUtil.moveFileToFolder(file,OUTPUT_FOLDER_PATH);
            }else if(ExcelFileUtil.isExtensionMatched(file,"xls") ||
                    ExcelFileUtil.isExtensionMatched(file,"xlsx")){
                FileUtils.deleteDirectory(new File(OUTPUT_FOLDER_PATH));
                ExcelFileUtil.convertExcelToCSV(file,OUTPUT_FOLDER_PATH, 1L, validate);
                List<Path> outPutFileList = ExcelFileUtil.listCsvFiles(OUTPUT_FOLDER_PATH, ".csv");
//                for(Path path : outPutFileList) {
//                    ExcelFileUtil.removeEmptyHeaderColumns(path);
//                }
            }
        }
    }
}
