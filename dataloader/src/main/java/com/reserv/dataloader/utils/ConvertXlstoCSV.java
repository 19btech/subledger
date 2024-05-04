package com.reserv.dataloader.utils;

import org.apache.commons.io.FilenameUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.usermodel.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import com.reserv.dataloader.exception.ExcelFormulaCellException;


@Component
public class ConvertXlstoCSV {

    Logger logger = LoggerFactory.getLogger(ConvertXlstoCSV.class);

    public File xlsx(File inputFile, int totalColumnCount, long docId) throws Exception {

        String tmpdir = System.getProperty("java.io.tmpdir");

        String fileNameWithOutExt = FilenameUtils.removeExtension(inputFile.getName());
        org.apache.commons.io.FileUtils.deleteQuietly(new File(tmpdir + File.separator + fileNameWithOutExt + ".csv"));
        File outputFile = new File(tmpdir + File.separator + fileNameWithOutExt + ".csv");
        xlsx(inputFile, outputFile, totalColumnCount, docId);
        return outputFile;
    }

    static String getColSeparator(int colIndex, int lastColIndex) {
        if (colIndex == lastColIndex - 1) {
            return "";
        } else {
            return ",";
        }
    }

    static void xlsx(File inputFile, File outputFile, int totalColumnCount, long docId) throws Exception {
        // For storing data into CSV files
        StringBuffer data = new StringBuffer();

        try {
            String ext = FilenameUtils.getExtension(inputFile.toString());

            if (ext.equalsIgnoreCase("xlsx")) {
                convertXLSXToCSV(inputFile, outputFile, totalColumnCount, docId);
            } else if (ext.equalsIgnoreCase("xls")) {
                convertXLSToCSV(inputFile, outputFile, totalColumnCount, docId);
            }

        } catch (Exception ioe) {
            ioe.printStackTrace();
            throw new ExcelFormulaCellException(ioe.getLocalizedMessage());
        }
    }

    static void convertXLSXToCSV(File inputFile, File outputFile, int totalColumnCount, long docId) throws Exception {
        // For storing data into CSV files
        StringBuffer data = new StringBuffer();

        try {
            // Get the workbook object for XLSX file
            FileInputStream fis = new FileInputStream(inputFile);
            Workbook workbook = WorkbookFactory.create(fis);
            fis.close();

            // Get first sheet from the workbook
            FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();

            int numberOfSheets = workbook.getNumberOfSheets();
            Row row;
            Cell cell;
            // Iterate through each rows from first sheet

            int colIndexCount = totalColumnCount;
            for (int i = 0; i < numberOfSheets; i++) {
                Sheet sheet = workbook.getSheetAt(0);
                Iterator<Row> rowIterator = sheet.iterator();
                copyRowDataIntoCSVFile(rowIterator,
                        outputFile,
                        docId,
                        colIndexCount,
                        evaluator);

            }

        } catch (Exception ioe) {
            ioe.printStackTrace();
            throw new ExcelFormulaCellException(ioe.getLocalizedMessage());
        }
    }

    static void convertXLSToCSV(File inputFile, File outputFile, int totalColumnCount, long docId) throws Exception {
        // For storing data into CSV files
        StringBuffer data = new StringBuffer();

        try {
            // Get the workbook object for XLSX file
            FileInputStream fis = new FileInputStream(inputFile);
            Workbook workbook = null;

            String ext = FilenameUtils.getExtension(inputFile.toString());

            workbook = new HSSFWorkbook(fis);
            fis.close();
            // Get first sheet from the workbook
            FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();

            int numberOfSheets = workbook.getNumberOfSheets();
            Row row;
            Cell cell;
            // Iterate through each rows from first sheet

            int colIndexCount = totalColumnCount;
            for (int i = 0; i < 1; i++) {
                Sheet sheet = workbook.getSheetAt(0);
                Iterator<Row> rowIterator = sheet.iterator();

                copyRowDataIntoCSVFile(rowIterator,
                        outputFile,
                        docId,
                        colIndexCount,
                        evaluator);

            }

        } catch (Exception ioe) {
            ioe.printStackTrace();
            throw new ExcelFormulaCellException(ioe.getLocalizedMessage());
        }
    }

    private static void copyRowDataIntoCSVFile(Iterator<Row> rowIterator,
                                               File outputFile,
                                               long docId,
                                               int colIndexCount,
                                               FormulaEvaluator evaluator) throws Exception  {
        try {

            FileOutputStream fos = new FileOutputStream(outputFile);
            // For storing data into CSV files
            StringBuffer data = new StringBuffer();
            int rowCounter = 0;
            Row row;
            Cell cell;

            while (rowIterator.hasNext()) {
                row = rowIterator.next();

                // For each row, iterate through each columns

                if (rowCounter == 0) {
                    data.append("\"docId\"" + ",");
                    data.append("\"csvRowId\"" + ",");
                    rowCounter++;
                } else {
                    data.append(docId + ",");
                    data.append(rowCounter + ",");
                    rowCounter++;
                }
                for (int cellIndex = 0; cellIndex < colIndexCount; cellIndex++) {

                    cell = row.getCell(cellIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);


                    switch (cell.getCellType()) {
                        case BOOLEAN:
                            data.append(cell.getBooleanCellValue() + getColSeparator(cellIndex, colIndexCount));
                            break;
                        case NUMERIC:
                            data.append(((DateUtil.isCellDateFormatted(cell)) ? getDateValue(cell) : cell.getNumericCellValue()) + getColSeparator(cellIndex, colIndexCount));
                            break;
                        case STRING:
                            data.append("\"" + String.valueOf(cell.getStringCellValue()).toUpperCase()
                                    + "\"" + getColSeparator(cellIndex, colIndexCount));
                            break;
                        case BLANK:
                            data.append("\"" + "\"" + getColSeparator(cellIndex, colIndexCount));
                            break;
                        case FORMULA:
                            setFormulaCellValue(cell, data, evaluator, cellIndex, colIndexCount);
                            break;
                        case _NONE:
                            data.append("\"" + "\"" + getColSeparator(cellIndex, colIndexCount));
                            break;
                        default:
                            data.append(cell + getColSeparator(cellIndex, colIndexCount));

                    }
                }
                data.append('\n'); // appending new line after each row
            }
            fos.write(data.toString().getBytes());
            fos.close();

        } catch(Exception ioe){
            ioe.printStackTrace();
            throw new ExcelFormulaCellException(ioe.getLocalizedMessage());
        }

    }

    private static void setFormulaCellValue(Cell cell,StringBuffer data,FormulaEvaluator evaluator,
                                            int cellIndex, int colIndexCount) throws Exception {

        try {
            CellValue cellValue = evaluator.evaluate(cell);
            switch(cellValue.getCellType()) {
                case BOOLEAN:
                    data.append(cellValue.getBooleanValue() + getColSeparator(cellIndex,colIndexCount));
                    break;
                case NUMERIC:
                    data.append(cellValue.getNumberValue() + getColSeparator(cellIndex,colIndexCount));
                    break;
                default:
                    data.append("\"" + cellValue.getStringValue() + "\""  + getColSeparator(cellIndex,colIndexCount));
            }
        }catch (Exception exp) {
            exp.printStackTrace();
            throw new ExcelFormulaCellException(exp.getLocalizedMessage());
        }

    }

    private static String getDateValue(Cell cell) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date date = cell.getDateCellValue();
        return  df.format(date);
    }


    public  void convertExcelToCSV(String filename, String filePath) throws Throwable{
        InputStream inp = null;
        try {
            DataFormatter formatter = new DataFormatter();
            inp = new FileInputStream(filename);
            Workbook wb = WorkbookFactory.create(inp);
            for (Sheet sheet : wb) {
                BufferedWriter writer = new BufferedWriter(new FileWriter(filePath + File.separator +  sheet.getSheetName() + ".csv", true));
                for (Row row : sheet) {
                    boolean firstCell = true;
                    for (Cell cell : row) {
                        if (!firstCell)
                            writer.append(',');
                        String text = formatter.formatCellValue(cell);
                        writer.append(text);
                        firstCell = false;
                    }
                    writer.append("\n");
                }
                writer.flush();;
                writer.close();
            }
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
            logger.error(ex.getLocalizedMessage());
            throw ex;
        } catch (UnsupportedEncodingException ex) {
            logger.error(ex.getLocalizedMessage());
            throw ex;
        } catch (IOException ex) {
            logger.error(ex.getLocalizedMessage());
            throw ex;
        } finally {
            try {
                inp.close();
            } catch (IOException ex) {
                logger.error(ex.getLocalizedMessage());
                throw ex;
            }
        }
    }
}
