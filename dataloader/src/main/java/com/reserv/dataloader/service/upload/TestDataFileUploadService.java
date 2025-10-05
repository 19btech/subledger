package com.reserv.dataloader.service.upload;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;
import com.fyntrac.common.entity.*;
import com.fyntrac.common.enums.DataActivityType;
import com.fyntrac.common.enums.DataFileType;
import com.fyntrac.common.service.*;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.ExcelUtil;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class TestDataFileUploadService extends FileUploadService {

    private static final Logger log = LoggerFactory.getLogger(TestDataFileUploadService.class);

    private final DataService<Class> dataService;
    private final SettingsService settingsService ;
    private final AccountingPeriodService accountingPeriodService;
    private final AccountingPeriodDataUploadService accountingPeriodDataUploadService;
    private final DataFileService dataFileService;

    private Map<String, Class> collectionMap = new HashMap<>(0);


    @Autowired
    TestDataFileUploadService(TenantContextHolder tenantContextHolder,
                              TransactionsUploadService transactionsUploadService,
                              ActivityUploadService activityUploadService,
                              DataService<Class> dataService,
                              AccountingPeriodService accountingPeriodService,
                              SettingsService settingsService,
                              AccountingPeriodDataUploadService accountingPeriodDataUploadService,
                              DataFileService dataFileService) {
        super(tenantContextHolder, transactionsUploadService, activityUploadService);
        this.dataService = dataService;
        this.settingsService = settingsService;
        this.accountingPeriodDataUploadService = accountingPeriodDataUploadService;
        this.accountingPeriodService = accountingPeriodService;
        this.dataFileService = dataFileService;

        try {
            initCollectionMap();
        } catch (Exception e) {
            log.error("Failed to initialize collectionMap", e);
            throw e;
        }
    }

    private void initCollectionMap() {
        collectionMap.put("TransactionActivity", TransactionActivity.class);
        collectionMap.put("InstrumentAttribute", InstrumentAttribute.class);
        collectionMap.put("Transactions", Transactions.class);
        collectionMap.put("Attributes", Attributes.class);
        collectionMap.put("Aggregation", Aggregation.class);
        collectionMap.put("ChartOfAccount", ChartOfAccount.class);
        collectionMap.put("SubledgerMapping", SubledgerMapping.class);
        collectionMap.put("AttributeBalance", AttributeLevelLtd.class);
        collectionMap.put("InstrumentBalance", InstrumentLevelLtd.class);

        // convert keys to uppercase
        collectionMap = collectionMap.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toUpperCase(),
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue,
                        LinkedHashMap::new
                ));
        log.debug("Initialized collectionMap with keys: {}", collectionMap.keySet());
    }

    @Override
    public void uploadFiles(MultipartFile... files) throws Throwable {
        log.info("Starting upload process for {} file(s)", files.length);

        Collection<AccountingPeriod> accountingPeriods = accountingPeriodService.getAccountingPeriods();
        if(accountingPeriods == null || accountingPeriods.isEmpty()) {
            Settings settings = settingsService.saveFiscalPriod(DateUtil.convertToDateFromYYYYMMDD(20000101));
            settingsService.generateAccountingPeriod(settings);
            this.accountingPeriodDataUploadService.generateAccountingPeriod(settings);
        }

        List<Workbook> workbooks = new ArrayList<>();
        try {
            for (MultipartFile file : files) {
                log.debug("Processing file: {}", file.getOriginalFilename());
                List<Workbook> tempList = ExcelUtil.extractWorkbooks(
                        file.getInputStream(),
                        file.getOriginalFilename()
                );
                log.debug("Extracted {} workbook(s) from {}", tempList.size(), file.getOriginalFilename());
                workbooks.addAll(tempList);
            }
        } catch (IOException e) {
            log.error("I/O error while reading uploaded files", e);
            throw new RuntimeException("Failed to read uploaded files", e);
        } catch (Exception e) {
            log.error("Unexpected error while extracting workbooks", e);
            throw e;
        }

        Set<String> sheetNames = new HashSet<>();
        for (Workbook workbook : workbooks) {
            try {
                List<String> tempSheetNames = ExcelUtil.getSheetNames(workbook);
                log.debug("Workbook contains sheets: {}", tempSheetNames);
                sheetNames.addAll(tempSheetNames);
            } catch (Exception e) {
                log.warn("Failed to read sheet names from workbook", e);
            }
        }

        Set<Class> collectionList = new HashSet<>();
        for (String sheetName : sheetNames) {
            Class collection = collectionMap.get(sheetName.toUpperCase());
            if (collection != null) {
                collectionList.add(collection);
                log.debug("Mapped sheet '{}' to collection '{}'", sheetName, collection.getSimpleName());
            } else {
                log.warn("No collection mapping found for sheet '{}'", sheetName);
            }
        }

        // Adding DataFiles collection name for data truncation
        // collectionList.add(DataFiles.class);
        for (Class collection : collectionList) {
            try {
                log.info("Truncating collection: {}", collection.getSimpleName());
                this.dataService.truncateCollection(collection);
            } catch (Exception e) {
                log.error("Failed to truncate collection: {}", collection.getSimpleName(), e);
                throw e; // rethrow to stop further processing
            }
        }

        try {
            List<File> inputFiles = new ArrayList<>(0);
            String OUTPUT_FOLDER_PATH = System.getProperty("user.home") + File.separator + "test" + File.separator + "output" + File.separator + "tenants" + File.separator + super.tenantContextHolder.getTenant() + File.separator;

            for(Workbook workbook :  workbooks) {
                    List<File> tmpFileList = ExcelUtil.splitSheetsToFiles(workbook, OUTPUT_FOLDER_PATH);
                inputFiles.addAll(tmpFileList);

            }
            super.uploadFiles(files);

            this.saveDataFiles(inputFiles);

            log.info("File upload process completed successfully");
        } catch (Throwable t) {
            log.error("Error in superclass file upload handling", t);
            throw t;
        }
    }

    public Collection<DataFiles> saveDataFiles(List<File> inputDataFiles) throws IOException {
        List<DataFiles> dataFiles = toDataFiles(DataActivityType.TEST, inputDataFiles);
        for(DataFiles dataFile : dataFiles) {
            this.dataFileService.deleteByFileName(dataFile.getFileName());
        }
        return this.dataService.saveAll(dataFiles, DataFiles.class);
    }
    public List<DataFiles> toDataFiles(DataActivityType activityType,
                                       List<File> files) {

        return files.stream()
                .map(file -> {
                    try {
                        DataFileType fileType = resolveDataFileType(file);
                        return DataFiles.builder()
                                .fileName(file.getName()) // ✅ File name
                                .contentType(Files.probeContentType(file.toPath())) // ✅ Guess content type
                                .size(file.length()) // ✅ File size
                                .content(Files.readAllBytes(file.toPath())) // ✅ File content
                                .uploadedAt(Instant.now())
                                .dataFileType(fileType)
                                .dataActivityType(activityType)
                                .createdAt(Instant.now())
                                .build();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to process file: " + file.getAbsolutePath(), e);
                    }
                })
                .collect(Collectors.toList());
    }

    public List<Records.DataFileRecord> getDataFileRecords(DataFileType dataFileType) {
        List<DataFiles> dataFiles = this.getDataFiles(dataFileType);

        List<Records.DataFileRecord> fileRecords = new ArrayList<>(0);

        for(DataFiles dataFile : dataFiles) {
            fileRecords.add(RecordFactory.createDataFileRecord(dataFile.getFileName(), dataFile.getId()));
        }

        return fileRecords;
    }


    public List<DataFiles> getDataFiles(DataFileType dataFileType) {
        return this.dataFileService.findByDataFileType(dataFileType);
    }


    public Optional<DataFiles> getDataFileById(String id) {
        return this.dataFileService.findById(id);
    }

    public static DataFileType resolveDataFileType(File file) {
        // Normalize the file name to uppercase to make checks case-insensitive
        String name = file.getName().toUpperCase();

        return switch (name) {
            case String s
                    when s.startsWith("TRANSACTIONACTIVITY")
                    || s.startsWith("INSTRUMENTATTRIBUTE")
                    || s.startsWith("ATTRIBUTEBALANCE")
                    || s.startsWith("INSTRUMENTBALANCE")
                    -> DataFileType.ACTIVITY_DATA;
            default -> DataFileType.REFERENCE_DATA;
        };
    }
}
