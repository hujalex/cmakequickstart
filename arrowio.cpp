#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <iostream>


arrow::Status GenInitialFile() {
  // Make a couple 8-bit integer arrays and a 16-bit integer array -- just like
  // basic Arrow example.

    int8_t arr[1000];
    for (int i = 0; i < 1000; ++i) {
        arr[i] = 30;
    }
    int16_t arr16[1000];
    for (int i = 0; i < 1000; ++i) {
        arr16[i] = 1000;
    }

  arrow::Int8Builder int8builder;
  int8_t days_raw[1000];
  std::copy(arr, arr + 1000, days_raw); //{1, 12, 17, 23, 28};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 1000));
  std::shared_ptr<arrow::Array> days;
  ARROW_ASSIGN_OR_RAISE(days, int8builder.Finish());

  int8_t months_raw[1000];
  std::copy(arr, arr + 1000, months_raw); // {1, 3, 5, 7, 1};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, 1000));
  std::shared_ptr<arrow::Array> months;
  ARROW_ASSIGN_OR_RAISE(months, int8builder.Finish());

  arrow::Int16Builder int16builder;
  int16_t years_raw[1000];
  std::copy(arr16, arr16 + 1000, years_raw);
  ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw, 1000));
  std::shared_ptr<arrow::Array> years;
  ARROW_ASSIGN_OR_RAISE(years, int16builder.Finish());

  // Get a vector of our Arrays
  std::vector<std::shared_ptr<arrow::Array>> columns = {days, months, years};

  // Make a schema to initialize the Table with
  std::shared_ptr<arrow::Field> field_day, field_month, field_year;
  std::shared_ptr<arrow::Schema> schema;

  field_day = arrow::field("Day", arrow::int8());
  field_month = arrow::field("Month", arrow::int8());
  field_year = arrow::field("Year", arrow::int16());

  schema = arrow::schema({field_day, field_month, field_year});
  // With the schema and data, create a Table
  std::shared_ptr<arrow::Table> table;
  table = arrow::Table::Make(schema, columns);

  // Write out test files in IPC, CSV, and Parquet for the example to use.
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.arrow"));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
                        arrow::ipc::MakeFileWriter(outfile, schema));
  ARROW_RETURN_NOT_OK(ipc_writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(ipc_writer->Close());

  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.csv"));
  ARROW_ASSIGN_OR_RAISE(auto csv_writer,
                        arrow::csv::MakeCSVWriter(outfile, table->schema()));
  ARROW_RETURN_NOT_OK(csv_writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(csv_writer->Close());

  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.parquet"));
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 5));

  return arrow::Status::OK();
}


arrow::Status RunMain() {

    ARROW_RETURN_NOT_OK(GenInitialFile());

    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(
        "test_in.arrow", arrow::default_memory_pool()));
    
    //Opening an Arrow File Reader

    ARROW_ASSIGN_OR_RAISE(auto ipc_reader, arrow::ipc::RecordBatchFileReader::Open(infile));

    std::shared_ptr<arrow::RecordBatch> rbatch;
    ARROW_ASSIGN_OR_RAISE(rbatch, ipc_reader->ReadRecordBatch(0));

    //Output file stream
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_out.arrow"));

    //output file writer
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
    arrow::ipc::MakeFileWriter(outfile, rbatch->schema()));

    //write record batch
    ARROW_RETURN_NOT_OK(ipc_writer->WriteRecordBatch(*rbatch));

    ARROW_RETURN_NOT_OK(ipc_writer->Close());

    //-----CSV-----
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open("test_in.csv"));
    //Prepare table
    std::shared_ptr<arrow::Table> csv_table;
    //Read CSV File to Table
    ARROW_ASSIGN_OR_RAISE(
        auto csv_reader,
        arrow::csv::TableReader::Make(
            arrow::io::default_io_context(), infile, arrow::csv::ReadOptions::Defaults(),
            arrow::csv::ParseOptions::Defaults(), arrow::csv::ConvertOptions::Defaults()));
    
    ARROW_ASSIGN_OR_RAISE(csv_table, csv_reader->Read());

    //Write a CSV file from table
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_out.csv"));
    ARROW_ASSIGN_OR_RAISE(auto csv_writer, arrow::csv::MakeCSVWriter(outfile, csv_table->schema()));
    ARROW_RETURN_NOT_OK(csv_writer->WriteTable(*csv_table));
    ARROW_RETURN_NOT_OK(csv_writer->Close());

    //-----Parquet------
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open("test_in.parquet"));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    //parquet reader
    PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));
    //read to table
    std::shared_ptr<arrow::Table> parquet_table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&parquet_table));

    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_out.parquet"));
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
        *parquet_table, arrow::default_memory_pool(), outfile, 1000 //chunk size changes storage size
    ));

    return arrow::Status::OK();
}

int main() {
    arrow::Status st = RunMain();
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    return 0;
}