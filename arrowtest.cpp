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

void appendSingleUnitData(std::vector<float> &arr) {
    arr.push_back(3.1415f);
}

arrow::Status buildCol(std::shared_ptr<arrow::Array> &col, int64_t length) {

    std::vector<float> arr;

    for (int i = 0; i < length; ++i) {
        appendSingleUnitData(arr);
    }

    arrow::FloatBuilder floatbuilder(arrow::float32(), arrow::default_memory_pool());
    ARROW_RETURN_NOT_OK(floatbuilder.Reserve(length));

    ARROW_RETURN_NOT_OK(floatbuilder.AppendValues(arr.data(), length));
    ARROW_ASSIGN_OR_RAISE(col, floatbuilder.Finish());
    return arrow::Status::OK();
}

arrow::Status buildData(std::vector<std::shared_ptr<arrow::Array>> & columns, int64_t length) {
    
    std::vector<std::thread> threads;
    std::vector<arrow::Status> statuses(columns.size());
    threads.reserve(columns.size());

    for (size_t i = 0; i < columns.size(); i++) {
        threads.emplace_back([&columns, &statuses, i, length]() {
            statuses[i] = buildCol(columns[i], length);
        });
    }

    for (auto& thread:threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    for (const auto& status: statuses) {
        ARROW_RETURN_NOT_OK(status);
    }

    return arrow::Status::OK();
}

arrow::Status writeData(std::shared_ptr<arrow::Schema> &schema,
std::vector<std::shared_ptr<arrow::Array>> &columns) 
{
   std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, columns);

    auto write_options = arrow::ipc::IpcWriteOptions::Defaults();
    write_options.use_threads = true;
    // write_options.memory_pool = arrow::default_memory_pool();


    ARROW_ASSIGN_OR_RAISE(auto outfile,
        arrow::io::FileOutputStream::Open("test_out.arrow"));

    ARROW_ASSIGN_OR_RAISE(auto buffered_outfile,
        arrow::io::BufferedOutputStream::Create(
            64 * 1024,
            arrow::default_memory_pool(),
            outfile
        )
    );

    ARROW_ASSIGN_OR_RAISE(auto ipc_writer,
        arrow::ipc::MakeFileWriter(outfile, schema, write_options));
    ARROW_RETURN_NOT_OK(ipc_writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(ipc_writer->Close());


    return arrow::Status::OK();
}



arrow::Status RunMain() {
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::Array>> columns(10);

    int64_t length = 1000000;

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