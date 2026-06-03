package com.reserv.dataloader.service.upload;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.entity.AccountingPeriod;
import com.fyntrac.common.entity.CustomTableDefinition;
import com.fyntrac.common.entity.ExecutionState;
import com.fyntrac.common.enums.AccountingRules;
import com.fyntrac.common.repository.CustomTableDefinitionRepository;
import com.fyntrac.common.service.AccountingPeriodService;
import com.fyntrac.common.service.ExecutionStateService;
import com.fyntrac.common.utils.DateUtil;
import com.fyntrac.common.utils.FileUtil;
import com.reserv.dataloader.exception.AccountingPeriodClosedException;
import com.reserv.dataloader.exception.CustomTableNotFoundException;
import com.reserv.dataloader.utils.ExcelFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Criteria;
import com.reserv.dataloader.service.model.ModelExecutionService;

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
    private final ExecutionStateService executionStateService;
    private final AccountingPeriodService accountingPeriodService;
    private final MongoTemplate mongoTemplate;
    private final ModelExecutionService modelExecutionService;

    FileUploadService(TenantContextHolder tenantContextHolder
            , ActivityUploadService activityUploadService,
                      TransactionsUploadService transactionsUploadService,
                      CustomTableDefinitionRepository customTableDefinitionRepository,
                      ExecutionStateService executionStateService,
                      AccountingPeriodService accountingPeriodService,
                      MongoTemplate mongoTemplate,
                      ModelExecutionService modelExecutionService) {
        this.tenantContextHolder = tenantContextHolder;
        this.activityUploadService = activityUploadService;
        this.customTableDefinitionRepository = customTableDefinitionRepository;
        this.transactionsUploadService = transactionsUploadService;
        this.executionStateService = executionStateService;
        this.accountingPeriodService = accountingPeriodService;
        this.mongoTemplate = mongoTemplate;
        this.modelExecutionService = modelExecutionService;
    }
    public void uploadFiles(boolean isOverwrite, MultipartFile ... files) throws Throwable {

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

        // Pre-populate the activity map to retain combined processing
        Map<AccountingRules, String> activityMap = new HashMap<>(0);
        for (Map.Entry<AccountingRules, String> entry : sortedMap.entrySet()) {
            AccountingRules rule = entry.getKey();
            if (rule == AccountingRules.TRANSACTIONACTIVITY || rule == AccountingRules.INSTRUMENTATTRIBUTE) {
                activityMap.put(rule, entry.getValue());
            }
        }

        // Execute jobs in sequential manner based on their sorted priority (highest first)
        boolean activityProcessed = false;
        for (Map.Entry<AccountingRules, String> entry : sortedMap.entrySet()) {
            AccountingRules rule = entry.getKey();
            boolean isActivity = (rule == AccountingRules.TRANSACTIONACTIVITY || rule == AccountingRules.INSTRUMENTATTRIBUTE);
            String file = entry.getValue();

            if (isActivity) {
                if (!activityProcessed) {
                    if (!activityMap.isEmpty()) {
                        log.info("Validating activity files before upload.");
                        validateActivityFiles(activityMap);
                        if (isOverwrite) {
                            ExecutionState executionState = executionStateService.getExecutionState();
                            if (executionState != null && executionState.getExecutionDate() != null && executionState.getExecutionDate() > 0) {
                                modelExecutionService.cleanupDataForPostingDate(executionState.getExecutionDate(), Boolean.FALSE, Boolean.TRUE);
                            }
                        }
                        log.info("Executing sequential activity upload at priority position.");
                        this.activityUploadService.uploadActivity(uploadId, activityMap);
                    }
                    activityProcessed = true;
                }
            } else {
                log.info("Executing sequential file upload for rule: {} (Priority: {})", rule, rule.getPriority());
                UploadService uploadService = UploadServiceFactory.getFileUploader(rule);
                uploadService.uploadData(uploadId, file);
            }
        }


    }


    /**
     * Validates the activity data files (InstrumentAttribute and/or TransactionActivity)
     * against the currently active ExecutionState before batch processing begins.
     *
     * <p>Validation rules:</p>
     * <ol>
     *   <li>The active ExecutionState.executionDate must not fall in a closed AccountingPeriod.
     *       If it does, {@link AccountingPeriodClosedException} is thrown.</li>
     *   <li>Every postingDate found in the uploaded files must be &gt;= ExecutionState.executionDate.
     *       If any row carries a postingDate that pre-dates the execution date,
     *       an {@link IllegalArgumentException} is thrown immediately (early-exit).</li>
     * </ol>
     *
     * @param activityMap map of AccountingRules (INSTRUMENTATTRIBUTE / TRANSACTIONACTIVITY)
     *                    to their resolved CSV file paths on disk
     * @throws AccountingPeriodClosedException if the execution date's accounting period is closed
     * @throws IllegalArgumentException        if any file contains a postingDate &lt; executionDate
     * @throws Exception                       on I/O or service errors
     */
    private void validateActivityFiles(Map<AccountingRules, String> activityMap)
            throws AccountingPeriodClosedException, Exception {

        // ── Fetch active ExecutionState (endDate == null) ────────────────────────
        ExecutionState executionState = executionStateService.getExecutionState();
        Integer executionDate = executionState.getExecutionDate();

        // ── Check 1: executionDate must not fall in a closed AccountingPeriod ────
        if (executionDate != null && executionDate > 0) {
            int accountingPeriodId = DateUtil.getAccountingPeriodId(executionDate);
            AccountingPeriod accountingPeriod = accountingPeriodService.getAccountingPeriod(accountingPeriodId);
            if (accountingPeriod != null && accountingPeriod.getStatus() == 1) {
                throw new AccountingPeriodClosedException(
                        "Upload rejected: the current execution date [" + executionDate +
                        "] falls within a closed accounting period [" + accountingPeriod + "]. " +
                        "Please advance the execution date before uploading activity data.");
            }
        }

        // ── Check 2: every postingDate in the uploaded files must be >= executionDate ──
        // Skip the scan entirely when no executionDate is set yet (system bootstrap).
        if (executionDate != null && executionDate > 0) {
            for (Map.Entry<AccountingRules, String> entry : activityMap.entrySet()) {
                AccountingRules rule = entry.getKey();
                String filePath    = entry.getValue();

                if (filePath == null || filePath.isBlank()) {
                    continue;
                }

                log.info("Validating postingDate in file [{}] for rule [{}]", filePath, rule);
                // Throws IllegalArgumentException on first offending row (early-exit)
                assertNoPostingDateBeforeExecutionDate(filePath, rule.name(), executionDate);
            }
        }

        log.info("Activity file validation passed (executionDate={}).", executionDate);
    }

    /**
     * Streams through a CSV file using a large {@link BufferedReader} and throws
     * {@link IllegalArgumentException} as soon as the first row whose POSTINGDATE
     * column is earlier than {@code executionDate} is encountered.
     *
     * <p><strong>Performance notes for large files (millions of rows):</strong></p>
     * <ul>
     *   <li>Uses an 8 MB I/O buffer to minimise system-call overhead.</li>
     *   <li>Only the single POSTINGDATE token is parsed per line — no per-row object
     *       allocation (unlike a CSV framework that builds record objects).</li>
     *   <li>Early-exit on the first violation: invalid files abort immediately;
     *       valid files are scanned once but with minimal per-row work.</li>
     * </ul>
     *
     * @param filePath      absolute path to the CSV file on disk
     * @param fileLabel     human-readable label for this file (rule name or table name), used in error messages
     * @param executionDate the active executionDate in YYYYMMDD integer format
     * @throws IllegalArgumentException if any row has postingDate &lt; executionDate
     */
    private void assertNoPostingDateBeforeExecutionDate(String filePath,
                                                        String fileLabel,
                                                        int executionDate) {
        // 8 MB read buffer — reduces OS read() calls dramatically for large files
        final int BUFFER_SIZE = 8 * 1024 * 1024;

        try (InputStream fis     = Files.newInputStream(Path.of(filePath));
             InputStream bis     = new BufferedInputStream(fis, BUFFER_SIZE);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(bis, StandardCharsets.UTF_8), BUFFER_SIZE)) {

            // ── Locate the POSTINGDATE column index from the header row ──────────
            String headerLine = reader.readLine();
            if (headerLine == null) {
                log.warn("File [{}] is empty; skipping postingDate validation.", filePath);
                return;
            }

            int postingDateColIndex = findColumnIndex(headerLine, "POSTINGDATE");
            if (postingDateColIndex < 0) {
                log.warn("No POSTINGDATE column found in [{}]; skipping postingDate validation.", filePath);
                return;
            }

            // ── Stream through data rows — early-exit on first violation ─────────
            long lineNumber = 1;
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }

                String rawValue = extractColumn(line, postingDateColIndex);
                if (rawValue == null || rawValue.isBlank()) {
                    continue;
                }

                try {
                    int postingDate = Integer.parseInt(rawValue);
                    if (postingDate < executionDate) {
                        // Fail fast — no need to read the rest of the file
                        throw new IllegalArgumentException(
                                "Upload rejected for [" + fileLabel + "]: row " + lineNumber +
                                " has postingDate [" + postingDate + "] which is earlier than" +
                                " the current executionDate [" + executionDate + "]. " +
                                "All records must have a postingDate >= executionDate.");
                    }
                } catch (NumberFormatException nfe) {
                    log.warn("Non-numeric POSTINGDATE value [{}] at line {} in [{}]; skipping row.",
                            rawValue, lineNumber, filePath);
                }
            }

            log.info("postingDate validation passed for [{}] ({} data lines scanned).",
                    filePath, lineNumber - 1);

        } catch (IllegalArgumentException e) {
            throw e; // re-throw validation failures as-is
        } catch (Exception e) {
            log.error("Failed to validate POSTINGDATE in [{}]: {}", filePath, e.getMessage(), e);
            throw new RuntimeException(
                    "Unable to validate postingDate in file [" + filePath + "]: " + e.getMessage(), e);
        }
    }

    /**
     * Finds the 0-based index of a column by name in a raw CSV header line.
     * Comparison is case-insensitive; surrounding quotes and whitespace are stripped.
     *
     * @param headerLine raw header line from the CSV file
     * @param columnName column name to search for (case-insensitive)
     * @return 0-based column index, or {@code -1} if not found
     */
    private static int findColumnIndex(String headerLine, String columnName) {
        String[] headers = headerLine.split(",", -1);
        for (int i = 0; i < headers.length; i++) {
            String h = headers[i].trim().replace("\"", "");
            if (h.equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Extracts the value at {@code colIndex} from a raw CSV line using a simple
     * comma split. Values are trimmed and unquoted.
     *
     * <p>Note: this intentionally avoids a full CSV parse to keep per-row cost
     * as low as possible for multi-million row files. It is sufficient here
     * because POSTINGDATE values are plain integers with no embedded commas.</p>
     *
     * @param line     raw data line
     * @param colIndex 0-based column index
     * @return trimmed, unquoted column value, or {@code null} if the index is out of range
     */
    private static String extractColumn(String line, int colIndex) {
        String[] cols = line.split(",", -1);
        if (colIndex >= cols.length) {
            return null;
        }
        return cols[colIndex].trim().replace("\"", "");
    }

    public void uploadCustomTableDataFiles(boolean isOverwrite, MultipartFile ... files) throws Throwable {

        String FOLDER_PATH = System.getProperty("user.home") + File.separator + "tenants" + File.separator + tenantContextHolder.getTenant() + File.separator;
        String OUTPUT_FOLDER_PATH = System.getProperty("user.home") + File.separator + "output" + File.separator + "tenants" + File.separator + tenantContextHolder.getTenant() + File.separator;

        FileUtil.createDirectoryIfNotExists(FOLDER_PATH);
        FileUtil.createDirectoryIfNotExists(OUTPUT_FOLDER_PATH);
        Set<String> fileSet = new HashSet<>(0);
        Set<String> validFileSet = new HashSet<>(0);
        boolean isValidFile = Boolean.FALSE;

        for(MultipartFile file : files) {
            if(ExcelFileUtil.isZipFile(file)) {
                Set<File> dataFiles =  ExcelFileUtil.unzip(file, FOLDER_PATH);
                for (File f : dataFiles) {
                    fileSet.add(f.getAbsolutePath());
                }
            }else {
                fileSet.add(ExcelFileUtil.convertMultipartFileToFile(file,FOLDER_PATH));
            }
        }

        this.convertIntoCSVFiles(fileSet, Boolean.FALSE);
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
                    validFileSet.add(fileName);
                }
            }
        }


        if (validFileSet.isEmpty()) {
            throw new CustomTableNotFoundException(
                    "No Custom table found "
            );
        } else if(!customTableMap.isEmpty()) {
            log.info("Validating custom table files before upload.");
            validateCustomTableFiles(customTableMap);
            if (isOverwrite) {
                ExecutionState executionState = executionStateService.getExecutionState();
                if (executionState != null && executionState.getExecutionDate() != null && executionState.getExecutionDate() > 0) {
                    int executionDate = executionState.getExecutionDate();
                    for (CustomTableDefinition tableDef : customTableMap.keySet()) {
                        if (tableDef.getTableType() == com.fyntrac.common.enums.CustomTableType.OPERATIONAL) {
                            try {
                                String collectionName = tableDef.getTableName();
                                Query query = new Query(Criteria.where("postingDate").is(executionDate));
                                mongoTemplate.remove(query, collectionName);
                                log.info("Purged data from custom table [{}] for postingDate [{}] due to overwrite option.", collectionName, executionDate);
                            } catch (Exception ex) {
                                log.error("Failed to purge data for Custom Table {} postingDate={}: {}", tableDef.getTableName(), executionDate, ex.getMessage());
                            }
                        }
                    }
                }
            }
            this.activityUploadService.uploadCustomTableData(customTableMap);
        }


    }

    /**
     * Validates the custom-table data files against the currently active ExecutionState.
     * Validation is applied <strong>only to OPERATIONAL tables</strong>; REFERENCE tables
     * carry no postingDate and are skipped.
     *
     * <p>Validation rules (same gates as activity file validation):</p>
     * <ol>
     *   <li>The active ExecutionState.executionDate must not fall in a closed AccountingPeriod.
     *       Throws {@link AccountingPeriodClosedException} if violated.</li>
     *   <li>Every POSTINGDATE value in an OPERATIONAL file must be &gt;= executionDate.
     *       Throws {@link IllegalArgumentException} on the first offending row (early-exit).</li>
     * </ol>
     *
     * @param customTableMap map of CustomTableDefinition to resolved CSV file path
     * @throws AccountingPeriodClosedException if the execution date's accounting period is closed
     * @throws IllegalArgumentException        if any OPERATIONAL file contains postingDate &lt; executionDate
     */
    private void validateCustomTableFiles(Map<CustomTableDefinition, String> customTableMap)
            throws AccountingPeriodClosedException, Exception {

        // ── Fetch active ExecutionState (endDate == null) ────────────────────────
        ExecutionState executionState = executionStateService.getExecutionState();
        Integer executionDate = executionState.getExecutionDate();

        // ── Check 1: executionDate must not fall in a closed AccountingPeriod ────
        if (executionDate != null && executionDate > 0) {
            int accountingPeriodId = DateUtil.getAccountingPeriodId(executionDate);
            AccountingPeriod accountingPeriod = accountingPeriodService.getAccountingPeriod(accountingPeriodId);
            if (accountingPeriod != null && accountingPeriod.getStatus() == 1) {
                throw new AccountingPeriodClosedException(
                        "Upload rejected: the current execution date [" + executionDate +
                        "] falls within a closed accounting period [" + accountingPeriod + "]. " +
                        "Please advance the execution date before uploading custom table data.");
            }
        }

        // ── Check 2: OPERATIONAL files only — every postingDate must be >= executionDate ──
        // Skip the scan entirely when no executionDate is set yet (system bootstrap).
        if (executionDate == null || executionDate == 0) {
            log.info("No executionDate set; skipping postingDate validation for custom tables.");
            return;
        }

        for (Map.Entry<CustomTableDefinition, String> entry : customTableMap.entrySet()) {
            CustomTableDefinition tableDef = entry.getKey();
            String filePath              = entry.getValue();

            // REFERENCE tables have no postingDate column — skip entirely
            if (tableDef.getTableType() != com.fyntrac.common.enums.CustomTableType.OPERATIONAL) {
                log.debug("Skipping postingDate validation for REFERENCE table [{}].", tableDef.getTableName());
                continue;
            }

            if (filePath == null || filePath.isBlank()) {
                continue;
            }

            log.info("Validating postingDate in OPERATIONAL custom table file [{}] for table [{}]",
                    filePath, tableDef.getTableName());
            // Reuse the same fast early-exit scanner from activity validation
            assertNoPostingDateBeforeExecutionDate(filePath, tableDef.getTableName(), executionDate);
        }

        log.info("Custom table file validation passed (executionDate={}).", executionDate);
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
