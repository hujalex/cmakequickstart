#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <memory>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <iostream>


arrow::Status createSchema(std::shared_ptr<arrow::Schema> & schema) {
    
    schema = arrow::schema({
        arrow::field("col1", arrow::float32()),
        arrow::field("col2", arrow::float32()),
        arrow::field("col3", arrow::float32()),
        arrow::field("col4", arrow::float32()),
        arrow::field("col5", arrow::float32()),
        arrow::field("col6", arrow::float32()),
        arrow::field("col7", arrow::float32()),
        arrow::field("col8", arrow::float32()),
        arrow::field("col9", arrow::float32()),
        arrow::field("col10", arrow::float32()),
    });
    return arrow::Status::OK();
}

arrow::Status buildCol(std::shared_ptr<arrow::Array> &col) {

    float arr[1000];
    for (int i = 0; i < 1000; ++i) {
        arr[i] = 3.1415f;
    }

    arrow::FloatBuilder floatbuilder(arrow::float32(), arrow::default_memory_pool());
    ARROW_RETURN_NOT_OK(floatbuilder.AppendValues(arr, 1000));
    ARROW_ASSIGN_OR_RAISE(col, floatbuilder.Finish());
    return arrow::Status::OK();
}

arrow::Status buildData(std::vector<std::shared_ptr<arrow::Array>> & columns) {
    // Create vector of 10 arrays
    // Build columns in loop
    for (auto& col : columns) {
        ARROW_RETURN_NOT_OK(buildCol(col));
    }

    return arrow::Status::OK();
}

arrow::Status writeData(std::shared_ptr<arrow::Schema> &schema,
std::vector<std::shared_ptr<arrow::Array>> &columns) 
{
    std::shared_ptr<arrow::RecordBatch> rbatch;
    rbatch = arrow::RecordBatch::Make(schema, 1000, columns);

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_out.arrow"));
    //output file writer
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
    arrow::ipc::MakeFileWriter(outfile, rbatch->schema()));
    //write record batch
    ARROW_RETURN_NOT_OK(ipc_writer->WriteRecordBatch(*rbatch));
    ARROW_RETURN_NOT_OK(ipc_writer->Close());

    return arrow::Status::OK();
}

arrow::Status RunMain() {
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::Array>> columns(10);

    ARROW_RETURN_NOT_OK(createSchema(schema));
    ARROW_RETURN_NOT_OK(buildData(columns));
    ARROW_RETURN_NOT_OK(writeData(schema, columns));

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