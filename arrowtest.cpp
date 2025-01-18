#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/writer.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <memory>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <iostream>
#include <thread>
#include <vector>


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

arrow::Status buildCol(std::shared_ptr<arrow::Array> &col, int64_t length) {

    // float arr[1000];
    // for (int i = 0; i < 1000; ++i) {
    //     arr[i] = 3.1415f;
    // }

    std::vector<float> arr(length, 3.1415f);
    arrow::FloatBuilder floatbuilder(arrow::float32(), arrow::default_memory_pool());
    ARROW_RETURN_NOT_OK(floatbuilder.Reserve(length));

    ARROW_RETURN_NOT_OK(floatbuilder.AppendValues(arr.data(), length));
    ARROW_ASSIGN_OR_RAISE(col, floatbuilder.Finish());
    return arrow::Status::OK();
}

arrow::Status buildData(std::vector<std::shared_ptr<arrow::Array>> & columns, int64_t length) {
    // Create vector of 10 arrays
    // Build columns in loop
    int num_threads = std::thread::hardware_concurrency();
    ARROW_ASSIGN_OR_RAISE(auto pool, arrow::internal::ThreadPool::Make(num_threads));

    std::vector<arrow::Future<arrow::Status>> futures;
    futures.reserve(columns.size());

    for (size_t i = 0; i < columns.size(); ++i) {
        futures.emplace_back(pool->Submit([&columns, i, length]() -> arrow::Status {
            return buildCol(columns[i], length);
        }));
    }

    // for (auto& col : columns) {
    //     ARROW_RETURN_NOT_OK(buildCol(col, length));
    // }
    // for (int i = 0; i < futures.size(); ++i) {
    //     ARROW_RETURN_NOT_OK(futures[i].get());
    // }
    return arrow::Status::OK();
}

arrow::Status writeData(std::shared_ptr<arrow::Schema> &schema,
std::vector<std::shared_ptr<arrow::Array>> &columns) 
{
   std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, columns);

    auto write_options = arrow::ipc::IpcWriteOptions::Defaults();
    // write_options.compression = arrow::Compression::ZSTD;
    write_options.use_threads = true;

    std::shared_ptr<arrow::io::MemoryMappedFile> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::MemoryMappedFile::Create("test_out.arrow", table->num_rows() * table->num_columns() * sizeof(float)));

    ARROW_ASSIGN_OR_RAISE(auto ipc_writer,
        arrow::ipc::MakeFileWriter(outfile, schema, write_options));

    ARROW_RETURN_NOT_OK(ipc_writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(ipc_writer->Close());

    return arrow::Status::OK();
}

arrow::Status RunMain() {
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::Array>> columns(10);

    int64_t length = 1000;

    ARROW_RETURN_NOT_OK(createSchema(schema));
    ARROW_RETURN_NOT_OK(buildData(columns, length));
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